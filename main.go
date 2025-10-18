package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// 基本配置
var upgrader = websocket.Upgrader{
	// 为简单起见，放开同源限制
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Client,单个连接
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

// Hub,管理所有连接并负责广播
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = true
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
		case msg := <-h.broadcast:
			for c := range h.clients {
				select {
				case c.send <- msg:
				default:
					// 客户端阻塞，异常等
					delete(h.clients, c)
					close(c.send)
				}
			}
		}
	}
}

// 读循环：将客户端的消息转发到hub.broadcast
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(64 * 1024)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			// 出现读错误，如超时，短线，执行退出
			break
		}
		// 简单广播
		c.hub.broadcast <- message
	}
}

// 写循环：把hub的广播写回客户端，定时发送ping
func (c *Client) writePump() {
	ticker := time.NewTicker(50 * time.Second) //心跳
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				// hub关闭通道，断开
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			// 一次写一个文本消息
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			// 发送Ping，保持连接
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// http处理
func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil) //升级为websocket
	if err != nil {
		log.Println("upgrade:", err)
		return
	}
	client := &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, 256),
	}
	hub.register <- client

	// 连接两个goroutine，实现读写分离
	go client.writePump()
	go client.readPump()
}

func main() {
	hub := NewHub()
	go hub.Run()

	// Websocket路由
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})

	// 静态页面
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	addr := ":8080"
	log.Println("listening on", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
