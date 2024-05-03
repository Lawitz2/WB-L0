package main

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
)

func (ord *Order) DBAdd(db *pgxpool.Pool) error { // добавление в БД
	tx, err := db.Begin(context.Background())
	if err != nil {
		slog.Error("transaction begin error: ", err.Error())
		return err
	}
	defer tx.Rollback(context.Background())

	queryOrders := `INSERT INTO orders (
	             order_uid,
	             track_number,
	             entry,
	             delivery_name,
	             delivery_phone,
	             delivery_zip,
	             delivery_city,
	             delivery_address,
	             delivery_region,
	             delivery_email,
	             payment_transaction,
	             payment_request_id,
	             payment_currency,
	             payment_provider,
	             payment_amount,
	             payment_payment_dt,
	             payment_bank,
	             payment_delivery_cost,
	             payment_goods_total,
	             payment_custom_fee,
	             locale,
	             internal_signature,
	             customer_id,
	             delivery_service,
	             shardkey,
	             sm_id,
	             date_created,
	             oof_shard)
	             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)`

	_, err = tx.Exec(context.Background(), queryOrders, ord.OrderUid, ord.TrackNumber, ord.Entry, ord.Delivery.Name, ord.Delivery.Phone, ord.Delivery.Zip, ord.Delivery.City, ord.Delivery.Address,
		ord.Delivery.Region, ord.Delivery.Email, ord.Payment.Transaction, ord.Payment.RequestId, ord.Payment.Currency, ord.Payment.Provider, ord.Payment.Amount, ord.Payment.PaymentDt,
		ord.Payment.Bank, ord.Payment.DeliveryCost, ord.Payment.GoodsTotal, ord.Payment.CustomFee, ord.Locale, ord.InternalSignature, ord.CustomerId, ord.DeliveryService,
		ord.Shardkey, ord.SmId, ord.DateCreated, ord.OofShard)

	if err != nil {
		slog.Error("couldn't exec query", err.Error())
		return err
	}

	queryItems := `INSERT INTO items (
                   chrt_id,
                   track_number,
                   price,
                   rid,
                   name,
                   sale,
                   size,
                   total_price,
                   nm_id,
                   brand,
                   status)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	for _, i := range ord.Items {
		_, err = tx.Exec(context.Background(), queryItems, i.ChrtId, i.TrackNumber, i.Price, i.Rid, i.Name, i.Sale, i.Size, i.TotalPrice, i.NmId, i.Brand, i.Status)
		if err != nil {
			slog.Error("couldn't write items: ", err.Error())
			return err
		}
	}

	tx.Commit(context.Background())
	return nil
}

func PullFromDB(uid string) (Order, error) {
	// если uid == "" - первоначальная загрузка в кэш
	// если uid не пустая строка, то получаем данные о заказе на основе uid из БД
	var rows pgx.Rows
	var err error

	tx, err := db.Begin(context.Background())
	if err != nil {
		slog.Error("couldnt begin transaction ", err.Error())
		return Order{}, err
	}
	defer tx.Rollback(context.Background())

	if uid == "" {
		rows, err = db.Query(context.Background(), "SELECT * FROM orders ORDER BY date_created DESC LIMIT $1", maxEntries)
		if err != nil {
			slog.Error("error getting rows", err.Error())
			return Order{}, err
		}
	} else {
		rows, err = db.Query(context.Background(), "SELECT * FROM orders WHERE order_uid = $1", uid)
		if err != nil {
			slog.Error("error getting rows", err.Error())
			return Order{}, err
		}
	}

	defer rows.Close()

	var file Order
	for rows.Next() {
		file = Order{}
		file.Items = make([]Item, 0)

		err := rows.Scan(&file.OrderUid, &file.TrackNumber, &file.Entry, &file.Delivery.Name, &file.Delivery.Phone, &file.Delivery.Zip, &file.Delivery.City, &file.Delivery.Address,
			&file.Delivery.Region, &file.Delivery.Email, &file.Payment.Transaction, &file.Payment.RequestId, &file.Payment.Currency, &file.Payment.Provider, &file.Payment.Amount, &file.Payment.PaymentDt,
			&file.Payment.Bank, &file.Payment.DeliveryCost, &file.Payment.GoodsTotal, &file.Payment.CustomFee, &file.Locale, &file.InternalSignature, &file.CustomerId, &file.DeliveryService,
			&file.Shardkey, &file.SmId, &file.DateCreated, &file.OofShard)
		if err != nil {
			continue // ошибка парсинга - пропускаем
		}

		rowsItems, _ := db.Query(context.Background(), "SELECT * FROM items where track_number = $1", file.TrackNumber)
		for rowsItems.Next() {
			var i Item
			err = rowsItems.Scan(&i.ChrtId, &i.TrackNumber, &i.Price, &i.Rid, &i.Name, &i.Sale, &i.Size, &i.TotalPrice, &i.NmId, &i.Brand, &i.Status)
			if err != nil {
				break // ошибка парсинга - выходим из цикла
			}
			file.Items = append(file.Items, i)
		}
		if err == nil { // если всё хорошо - пишем в кеш
			myCache.Set(file.OrderUid, file)
		}
		rowsItems.Close()
	}
	tx.Commit(context.Background())
	return file, nil
}
