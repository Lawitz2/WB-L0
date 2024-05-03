package main

import (
	"encoding/json"
	"fmt"
	"github.com/jackc/pgerrcode"
	_ "github.com/jackc/pgerrcode"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
	"net/mail"
	"strings"
	"time"
)

type Order struct {
	Delivery          Delivery  `json:"Delivery"`
	Payment           Payment   `json:"Payment"`
	Items             []Item    `json:"items"`
	OrderUid          string    `json:"order_uid"`
	TrackNumber       string    `json:"track_number"`
	Entry             string    `json:"entry"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerId        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmId              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestId    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtId      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmId        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

func Jsonread(msg jetstream.Msg) {
	var file Order

	err := json.Unmarshal(msg.Data(), &file)
	if err != nil {
		slog.Warn("couldn't unmarshal json")
		msg.Ack()
		return
	}

	_, err = mail.ParseAddress(file.Delivery.Email)
	if err != nil {
		slog.Warn("incorrect email format")
		msg.Ack()
		return
	}

	err = file.DBAdd(db) // если анмаршал без ошибок - добавляем полученный из сообщения файл в бд
	if err != nil {
		if strings.Contains(err.Error(), pgerrcode.UniqueViolation) {
			fmt.Println("duplicate key")
			msg.Ack()
			return
		}
		slog.Error("error writing to db: ", err.Error())
		msg.Nak()
		return
	}

	myCache.Set(file.OrderUid, file) // добавляем файл в кэш

	msg.Ack()
}
