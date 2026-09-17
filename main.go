package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Global Configuration
var (
	db          *sql.DB
	botToken    string
	publicURL   string
	adminChatID string
	httpClient  *http.Client
)

// Thread-safe In-Memory Cache for User Maps
type UserCacheEntry struct {
	ChatID     string
	UserKey    string
	MaxAlerts  int
	Expiration time.Time
}

var (
	userCache      = make(map[string]UserCacheEntry)
	userCacheMutex sync.RWMutex
)

// FIX 1: Proper cryptographically secure UID and Key generation
const uidAlphabet = "abcdefghjklmnpqrstuvwxyz23456789"
const keyAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateRandomString(alphabet string, length int) string {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		log.Fatal("crypto/rand failed:", err)
	}
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = alphabet[int(bytes[i])%len(alphabet)]
	}
	return string(result)
}

func main() {
	botToken = os.Getenv("TELEGRAM_BOT_TOKEN")
	publicURL = strings.TrimSuffix(os.Getenv("APP_PUBLIC_URL"), "/")
	if publicURL == "" {
		publicURL = "https://notifyu.me"
	}
	adminChatID = strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))

	dbURL := os.Getenv("SPRING_DATASOURCE_URL")
	if dbURL == "" {
		log.Fatal("CRITICAL: SPRING_DATASOURCE_URL is missing")
	}

	// Optimized HTTP Client with Keep-Alive
	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 10 * time.Second,
	}

	// Connect to Supabase Postgres
	var err error
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("Database unreachable: %v", err)
	}
	log.Println("✅ Supabase database connected successfully!")

	// Keepalive ping every 4 minutes to prevent idle connection drops
	go func() {
		ticker := time.NewTicker(4 * time.Minute)
		for range ticker.C {
			_ = db.Ping()
		}
	}()

	// Cache cleanup every 10 minutes
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		for range ticker.C {
			now := time.Now()
			userCacheMutex.Lock()
			for k, v := range userCache {
				if now.After(v.Expiration) {
					delete(userCache, k)
				}
			}
			userCacheMutex.Unlock()
		}
	}()

	// DB cleanup every 12 hours
	go func() {
		ticker := time.NewTicker(12 * time.Hour)
		for range ticker.C {
			_, err := db.Exec("DELETE FROM telegram_updates WHERE processed_at < NOW() - INTERVAL '1 day'")
			if err != nil {
				log.Printf("Cleanup error: %v", err)
			} else {
				log.Println("Database cleanup completed.")
			}
		}
	}()

	http.HandleFunc("/chartink", handleWebhook)
	http.HandleFunc("/telegram", handleTelegram)
	fileServer := http.FileServer(http.Dir("src/main/resources/static"))
	http.Handle("/", fileServer)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("🚀 Server running on port %s", port)
	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		log.Fatalf("Server crash: %v", err)
	}
}

// Chartink & TradingView Webhook Handler
func handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read full body first before anything else
	bodyBytes, _ := io.ReadAll(r.Body)
	bodyStr := string(bodyBytes)

	// Restore body for form parsing
	r.Body = io.NopCloser(strings.NewReader(bodyStr))
	r.ParseForm()

	contentType := r.Header.Get("Content-Type")

	// Try query params first (TradingView style)
	uid := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("uid")))
	key := strings.TrimSpace(r.URL.Query().Get("key"))

	// Fallback: form body fields (Chartink sends uid/key inside POST body)
	if uid == "" {
		uid = strings.TrimSpace(strings.ToLower(r.FormValue("uid")))
	}
	if key == "" {
		key = strings.TrimSpace(r.FormValue("key"))
	}

	// Fallback: JSON body (some integrations send everything as JSON)
	if uid == "" && strings.Contains(contentType, "application/json") {
		uid = strings.TrimSpace(strings.ToLower(extractValue(bodyStr, "uid")))
		key = strings.TrimSpace(extractValue(bodyStr, "key"))
	}

	// Fallback: raw text body manual extraction
	if uid == "" {
		uid = strings.TrimSpace(strings.ToLower(extractValue(bodyStr, "uid")))
	}
	if key == "" {
		key = strings.TrimSpace(extractValue(bodyStr, "key"))
	}

	if uid == "" {
		fmt.Fprint(w, "NO_UID")
		return
	}
	if key == "" {
		fmt.Fprint(w, "NO_KEY")
		return
	}

	// Check RAM cache first
	var chatID string
	var maxAlerts int
	cacheValid := false

	userCacheMutex.RLock()
	entry, found := userCache[uid]
	userCacheMutex.RUnlock()

	if found && time.Now().Before(entry.Expiration) {
		if entry.UserKey != key {
			fmt.Fprint(w, "FORBIDDEN")
			return
		}
		chatID = entry.ChatID
		maxAlerts = entry.MaxAlerts
		cacheValid = true
	}

	// Cache miss -> Query DB
	if !cacheValid {
		var userKey string
		err := db.QueryRow(
			"SELECT chat_id, user_key, COALESCE(max_alerts, 100) FROM user_map WHERE uid = $1", uid,
		).Scan(&chatID, &userKey, &maxAlerts)

		if err == sql.ErrNoRows {
			fmt.Fprint(w, "UID_NOT_LINKED")
			return
		} else if err != nil {
			log.Printf("DB Error: %v", err)
			fmt.Fprint(w, "OK")
			return
		}

		if userKey != key {
			fmt.Fprint(w, "FORBIDDEN")
			return
		}

		userCacheMutex.Lock()
		userCache[uid] = UserCacheEntry{
			ChatID:     chatID,
			UserKey:    userKey,
			MaxAlerts:  maxAlerts,
			Expiration: time.Now().Add(5 * time.Minute),
		}
		userCacheMutex.Unlock()
	}

	// Daily usage check
	todayStr := time.Now().Format("2006-01-02")
	var currentUsage int
	_ = db.QueryRow(
		"SELECT alerts_count FROM daily_usage WHERE chat_id = $1 AND day = $2", chatID, todayStr,
	).Scan(&currentUsage)

	if currentUsage >= maxAlerts {
		fmt.Fprint(w, "LIMIT_EXCEEDED")
		return
	}

	// Atomic increment
	_, _ = db.Exec(
		`INSERT INTO daily_usage(day, chat_id, alerts_count) VALUES($1, $2, 1)
		 ON CONFLICT (day, chat_id) DO UPDATE SET alerts_count = daily_usage.alerts_count + 1`,
		todayStr, chatID,
	)

	go sendTelegram(chatID, buildMessage(bodyStr))
	fmt.Fprint(w, "OK")
}

// Telegram Bot Command Handler
func handleTelegram(w http.ResponseWriter, r *http.Request) {
	// Instantly acknowledge Telegram to prevent retries
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")

	var update struct {
		UpdateID int64 `json:"update_id"`
		Message  *struct {
			Chat struct {
				ID int64 `json:"id"`
			} `json:"chat"`
			Text string `json:"text"`
		} `json:"message"`
	}

	if err := json.NewDecoder(r.Body).Decode(&update); err != nil || update.Message == nil || update.Message.Text == "" {
		return
	}

	// FIX 5: Atomic dedup using ON CONFLICT — same as Node.js version
	var rowsAffected int64
	result, err := db.Exec(
		"INSERT INTO telegram_updates (update_id) VALUES ($1) ON CONFLICT (update_id) DO NOTHING",
		update.UpdateID,
	)
	if err == nil {
		rowsAffected, _ = result.RowsAffected()
	}
	if rowsAffected == 0 {
		return // Duplicate update, ignore
	}

	chatIDStr := fmt.Sprintf("%d", update.Message.Chat.ID)
	text := strings.TrimSpace(update.Message.Text)
	isAdmin := (chatIDStr == adminChatID)

	// /start
	if strings.HasPrefix(text, "/start") {
		var uid, userKey string
		err := db.QueryRow("SELECT uid, user_key FROM user_map WHERE chat_id = $1", chatIDStr).Scan(&uid, &userKey)
		if err == nil {
			go sendTelegram(chatIDStr, fmt.Sprintf("✅ Already linked: `%s`\nUse /myuid for your Webhook URL.", uid))
		} else {
			newUid := generateRandomString(uidAlphabet, 8)
			newKey := generateRandomString(keyAlphabet, 24)
			_, dbErr := db.Exec(
				"INSERT INTO user_map(uid, chat_id, user_key, updated_at) VALUES($1,$2,$3,$4)",
				newUid, chatIDStr, newKey, time.Now().Unix(),
			)
			if dbErr != nil {
				log.Printf("Insert error: %v", dbErr)
				return
			}
			go sendTelegram(chatIDStr, buildLinkedMessage(newUid, newKey))
		}
		return
	}

	// /myuid
	if strings.HasPrefix(text, "/myuid") {
		var uid, userKey string
		err := db.QueryRow("SELECT uid, user_key FROM user_map WHERE chat_id = $1", chatIDStr).Scan(&uid, &userKey)
		if err != nil {
			go sendTelegram(chatIDStr, "⚠️ Not linked. Send /start to generate your URL.")
			return
		}
		go sendTelegram(chatIDStr, buildLinkedMessage(uid, userKey))
		return
	}

	// /unlink
	if strings.HasPrefix(text, "/unlink") {
		if !strings.Contains(text, "confirm") {
			go sendTelegram(chatIDStr, "⚠️ Send `/unlink confirm` to delete your link.")
			return
		}
		_, _ = db.Exec("DELETE FROM user_map WHERE chat_id = $1", chatIDStr)
		// Also clear from cache
		userCacheMutex.Lock()
		for k, v := range userCache {
			if v.ChatID == chatIDStr {
				delete(userCache, k)
			}
		}
		userCacheMutex.Unlock()
		go sendTelegram(chatIDStr, "❌ Unlinked successfully.")
		return
	}

	// /newuid
	if strings.HasPrefix(text, "/newuid") {
		if !strings.Contains(text, "confirm") {
			go sendTelegram(chatIDStr, "⚠️ Send `/newuid confirm` to rotate your URL.")
			return
		}
		// Clear old entry from cache
		var oldUid string
		_ = db.QueryRow("SELECT uid FROM user_map WHERE chat_id = $1", chatIDStr).Scan(&oldUid)
		if oldUid != "" {
			userCacheMutex.Lock()
			delete(userCache, oldUid)
			userCacheMutex.Unlock()
		}
		_, _ = db.Exec("DELETE FROM user_map WHERE chat_id = $1", chatIDStr)
		newUid := generateRandomString(uidAlphabet, 8)
		newKey := generateRandomString(keyAlphabet, 24)
		_, _ = db.Exec(
			"INSERT INTO user_map(uid, chat_id, user_key, updated_at) VALUES($1,$2,$3,$4)",
			newUid, chatIDStr, newKey, time.Now().Unix(),
		)
		go sendTelegram(chatIDStr, buildLinkedMessage(newUid, newKey))
		return
	}

	// /stats
	if strings.HasPrefix(text, "/stats") {
		todayStr := time.Now().Format("2006-01-02")
		var alertsCount int
		var maxAlerts int = 100
		_ = db.QueryRow("SELECT alerts_count FROM daily_usage WHERE chat_id = $1 AND day = $2", chatIDStr, todayStr).Scan(&alertsCount)
		_ = db.QueryRow("SELECT COALESCE(max_alerts, 100) FROM user_map WHERE chat_id = $1", chatIDStr).Scan(&maxAlerts)
		go sendTelegram(chatIDStr, fmt.Sprintf("📊 *Daily Usage*\nUsed: %d / %d", alertsCount, maxAlerts))
		return
	}

	// /more
	if strings.HasPrefix(text, "/more") {
		go sendTelegram(chatIDStr, "⚙️ *Other Actions*\n\n/newuid - Rotate URL\n/unlink - Delete account\n/support <message> - Contact support")
		return
	}

	// /support <message> (Available to all users)
	if strings.HasPrefix(text, "/support") {
		query := strings.TrimSpace(strings.TrimPrefix(text, "/support"))
		if query == "" {
			go sendTelegram(chatIDStr, "⚠️ Please provide a message after `/support`.\nExample:\n`/support How do I setup multiple scanners?`")
			return
		}
		if adminChatID != "" {
			adminMsg := fmt.Sprintf("📩 *New Support Request*\n• From Chat ID: `%s`\n\n%s\n\n👉 _Reply with:_ `/reply %s <your message>`", chatIDStr, query, chatIDStr)
			go sendTelegram(adminChatID, adminMsg)
		}
		go sendTelegram(chatIDStr, "✅ Your support request has been submitted. We will reply directly inside this chat!")
		return
	}

	// Admin commands
	if isAdmin {
		if strings.HasPrefix(text, "/reply") {
			parts := strings.Fields(text)
			if len(parts) < 3 {
				go sendTelegram(chatIDStr, "⚠️ Usage: `/reply <chat_id> <message>`")
				return
			}
			targetChat := strings.TrimSpace(parts[1])

			idx := strings.Index(text, parts[1])
			replyMsg := strings.TrimSpace(text[idx+len(parts[1]):])

			if replyMsg == "" {
				go sendTelegram(chatIDStr, "⚠️ Message cannot be empty.")
				return
			}

			go sendTelegram(targetChat, fmt.Sprintf("💬 *Support Response:*\n\n%s", replyMsg))
			go sendTelegram(chatIDStr, fmt.Sprintf("✅ Reply sent to `%s`", targetChat))
			return
		}

		if strings.HasPrefix(text, "/adminstats") {
			var totalUsers int
			var todayAlerts int
			todayStr := time.Now().Format("2006-01-02")
			_ = db.QueryRow("SELECT COUNT(*) FROM user_map").Scan(&totalUsers)
			_ = db.QueryRow("SELECT COALESCE(SUM(alerts_count), 0) FROM daily_usage WHERE day = $1", todayStr).Scan(&todayAlerts)
			go sendTelegram(chatIDStr, fmt.Sprintf("📊 *Admin Stats*\n\nTotal Users: %d\nAlerts Today: %d", totalUsers, todayAlerts))
			return
		}

		if strings.HasPrefix(text, "/admintop") {
			todayStr := time.Now().Format("2006-01-02")
			rows, err := db.Query(
				`SELECT um.uid, du.alerts_count FROM daily_usage du
				 JOIN user_map um ON um.chat_id = du.chat_id
				 WHERE du.day = $1 ORDER BY du.alerts_count DESC LIMIT 10`, todayStr,
			)
			if err != nil {
				go sendTelegram(chatIDStr, "❌ Failed to fetch top users.")
				return
			}
			defer rows.Close()

			sb := "🏆 *Top 10 Today*\n\n"
			count := 0
			for rows.Next() {
				var uid string
				var alertsCount int
				if err := rows.Scan(&uid, &alertsCount); err == nil {
					sb += fmt.Sprintf("• %s: %d\n", uid, alertsCount)
					count++
				}
			}
			if count == 0 {
				sb += "No alerts recorded today."
			}
			go sendTelegram(chatIDStr, sb)
			return
		}

		if strings.HasPrefix(text, "/alertlimit") {
			parts := strings.Fields(text)
			if len(parts) < 3 {
				go sendTelegram(chatIDStr, "⚠️ Usage: `/alertlimit <chat_id_or_uid> <limit>`")
				return
			}
			target := strings.TrimSpace(parts[1])
			newLimit, err := strconv.Atoi(strings.TrimSpace(parts[2]))
			if err != nil {
				go sendTelegram(chatIDStr, "❌ Invalid limit number.")
				return
			}

			result, err := db.Exec(
				"UPDATE user_map SET max_alerts = $1 WHERE chat_id = $2 OR LOWER(uid) = LOWER($3)",
				newLimit, target, target,
			)
			if err != nil {
				go sendTelegram(chatIDStr, "❌ Database error while updating limit.")
				return
			}

			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				// Invalidate RAM cache so new limit takes effect immediately
				userCacheMutex.Lock()
				for k, v := range userCache {
					if v.ChatID == target || strings.EqualFold(k, target) {
						delete(userCache, k)
					}
				}
				userCacheMutex.Unlock()

				go sendTelegram(chatIDStr, fmt.Sprintf("✅ Success! Alert limit updated to *%d* for target: `%s`", newLimit, target))
			} else {
				go sendTelegram(chatIDStr, fmt.Sprintf("❌ User mapping not found for identifier: `%s`", target))
			}
			return
		}

		if strings.HasPrefix(text, "/setlimitall") {
			parts := strings.Fields(text)
			if len(parts) < 2 {
				go sendTelegram(chatIDStr, "⚠️ Usage: `/setlimitall <limit>`")
				return
			}
			newLimit, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil {
				go sendTelegram(chatIDStr, "❌ Invalid limit number.")
				return
			}

			result, err := db.Exec("UPDATE user_map SET max_alerts = $1", newLimit)
			if err != nil {
				go sendTelegram(chatIDStr, "❌ Database error while updating limits.")
				return
			}

			rowsAffected, _ := result.RowsAffected()

			// Clear entire RAM cache so new limit applies immediately to everyone
			userCacheMutex.Lock()
			userCache = make(map[string]UserCacheEntry)
			userCacheMutex.Unlock()

			go sendTelegram(chatIDStr, fmt.Sprintf("✅ Alert limit updated to *%d* for *%d* users.", newLimit, rowsAffected))
			return
		}

		if strings.HasPrefix(text, "/sendmsg") {
			parts := strings.Fields(text)
			if len(parts) < 3 {
				go sendTelegram(chatIDStr, "⚠️ Usage: `/sendmsg <chat_id> <message>`")
				return
			}
			targetChat := strings.TrimSpace(parts[1])

			// Reconstruct the message text (everything after the chat_id)
			idx := strings.Index(text, parts[1])
			customMsg := strings.TrimSpace(text[idx+len(parts[1]):])

			if customMsg == "" {
				go sendTelegram(chatIDStr, "⚠️ Message cannot be empty.")
				return
			}

			go sendTelegram(targetChat, customMsg)
			go sendTelegram(chatIDStr, fmt.Sprintf("🚀 Message sent to `%s`:\n\n%s", targetChat, customMsg))
			return
		}

		if strings.HasPrefix(text, "/adminusers") {
			rows, err := db.Query("SELECT uid, chat_id FROM user_map ORDER BY updated_at DESC LIMIT 10")
			if err != nil {
				go sendTelegram(chatIDStr, "❌ Failed to fetch users.")
				return
			}
			defer rows.Close()

			sb := "👥 *Last 10 Registered Users*\n\n"
			count := 0
			for rows.Next() {
				var uid, chatID string
				if err := rows.Scan(&uid, &chatID); err == nil {
					sb += fmt.Sprintf("• %s | %s\n", uid, chatID)
					count++
				}
			}
			if count == 0 {
				sb += "No users registered yet."
			}
			go sendTelegram(chatIDStr, sb)
			return
		}
	}
}

func buildLinkedMessage(uid, userKey string) string {
	webhook := fmt.Sprintf("%s/chartink?uid=%s&key=%s", publicURL, uid, userKey)
	return fmt.Sprintf(
		"✅ *Linked Successfully!*\n\n*Webhook URL:* `%s`\n\nPaste this URL in Chartink/TradingView in the webhook field while setting alert\n\n/stats - Usage\n/more - Actions",
		webhook,
	)
}

// FIX 2: Full message parser matching Node.js version
func buildMessage(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return "🔔 *Alert Received*\n\nNo data payload found."
	}

	scanName := "External Alert"
	stockData := ""
	timePart := ""
	triggeredStocks := ""

	if strings.HasPrefix(body, "{") {
		// JSON payload (TradingView or Chartink JSON mode)
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(body), &raw); err == nil {
			if v, ok := raw["stocks"]; ok {
				triggeredStocks = fmt.Sprintf("%v", v)
			}
			symbol := ""
			if v, ok := raw["symbol"]; ok {
				symbol = fmt.Sprintf("%v", v)
			}
			if symbol == "" {
				if v, ok := raw["Value1"]; ok {
					symbol = fmt.Sprintf("%v", v)
				}
			}
			// NEW - handles both trigger_prices (Chartink) and trigger_price (TradingView)
			price := ""
			if v, ok := raw["trigger_prices"]; ok {
				price = fmt.Sprintf("%v", v)
			} else if v, ok := raw["trigger_price"]; ok {
				price = fmt.Sprintf("%v", v)
			}
			if symbol != "" {
				if price != "" {
					stockData = symbol + " @ " + price
				} else {
					stockData = symbol
				}
			}

			// Also show stocks with prices side by side if both available
			if triggeredStocks != "" && price != "" {
				stocks := strings.Split(triggeredStocks, ",")
				prices := strings.Split(price, ",")
				var combined []string
				for i, s := range stocks {
					s = strings.TrimSpace(s)
					if i < len(prices) {
						combined = append(combined, s+" @ "+strings.TrimSpace(prices[i]))
					} else {
						combined = append(combined, s)
					}
				}
				triggeredStocks = strings.Join(combined, ", ")
			}
			for _, key := range []string{"scan_name", "alert_name", "title"} {
				if v, ok := raw[key]; ok && fmt.Sprintf("%v", v) != "" {
					scanName = fmt.Sprintf("%v", v)
					break
				}
			}
			if v, ok := raw["triggered_at"]; ok {
				timePart = fmt.Sprintf("%v", v)
			}
		} else {
			// Fallback manual extraction if JSON parse fails
			triggeredStocks = extractValue(body, "stocks")
			symbol := extractValue(body, "symbol")
			if symbol == "" {
				symbol = extractValue(body, "Value1")
			}
			price := extractValue(body, "trigger_price")
			if symbol != "" {
				if price != "" {
					stockData = symbol + " @ " + price
				} else {
					stockData = symbol
				}
			}
			for _, key := range []string{"scan_name", "alert_name", "title"} {
				if v := extractValue(body, key); v != "" {
					scanName = v
					break
				}
			}
			timePart = extractValue(body, "triggered_at")
		}
	} else if strings.Contains(strings.ToLower(body), "extra data:") {
		// Chartink plain text format
		idx := strings.Index(strings.ToLower(body), "extra data:") + 11
		extra := strings.TrimSpace(body[idx:])
		parts := strings.Split(extra, ",")
		if len(parts) >= 1 {
			scanName = strings.TrimSpace(parts[0])
		}
		if len(parts) >= 2 {
			stockData = strings.TrimSpace(parts[1])
		}
		if atIdx := strings.Index(extra, "@"); atIdx != -1 {
			timePart = strings.TrimSpace(extra[atIdx:])
		}
	} else {
		stockData = body
	}

	sb := "🔔 *New Alert*\n\n"
	if scanName != "" {
		sb += fmt.Sprintf("🧠 *Scan:* %s\n", escapeMarkdown(scanName))
	}
	if stockData != "" {
		sb += fmt.Sprintf("📈 *Trigger:* %s\n", stockData)
	}
	if triggeredStocks != "" {
		sb += fmt.Sprintf("📋 *Full List:* %s\n", triggeredStocks)
	}
	if timePart != "" {
		sb += fmt.Sprintf("⏰ *Time:* %s\n", timePart)
	}
	return strings.TrimSpace(sb)
}

// Manual JSON value extractor (fallback)
func extractValue(jsonStr, key string) string {
	pattern := `"` + key + `":`
	start := strings.Index(jsonStr, pattern)
	if start == -1 {
		return ""
	}
	start += len(pattern)
	for start < len(jsonStr) && (jsonStr[start] == ' ' || jsonStr[start] == '"') {
		start++
	}
	end := strings.Index(jsonStr[start:], `"`)
	if end == -1 {
		endComma := strings.Index(jsonStr[start:], ",")
		endBrace := strings.Index(jsonStr[start:], "}")
		if endComma == -1 {
			end = endBrace
		} else if endBrace == -1 {
			end = endComma
		} else {
			if endComma < endBrace {
				end = endComma
			} else {
				end = endBrace
			}
		}
		if end == -1 {
			return ""
		}
		return strings.TrimSpace(jsonStr[start : start+end])
	}
	return strings.TrimSpace(jsonStr[start : start+end])
}

func escapeMarkdown(s string) string {
	replacer := strings.NewReplacer(
		"_", "\\_", "*", "\\*", "[", "\\[", "]", "\\]",
		"(", "\\(", ")", "\\)", "~", "\\~", "`", "\\`",
		">", "\\>", "#", "\\#", "+", "\\+", "-", "\\-",
		"=", "\\=", "|", "\\|", "{", "\\{", "}", "\\}",
		".", "\\.", "!", "\\!",
	)
	return replacer.Replace(s)
}

func sendTelegram(chatID, text string) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", botToken)
	payload, _ := json.Marshal(map[string]interface{}{
		"chat_id":    chatID,
		"text":       text,
		"parse_mode": "Markdown",
	})
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
