package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"istio.io/pkg/cache"
	"log/slog"
	"os"
	"os/signal"
	"time"
)

const (
	maxEntries = 2 // определяет максимальное кол-во элементов в кэше
)

var (
	myCache = cache.NewLRU(time.Hour, 5*time.Second, maxEntries) // кэш имеет конкурентно-безопасную реализацию
	db      *pgxpool.Pool
)

func RetrieveOrder(uid string) (Order, error) { // Получаем данные о заказе на основе order_uid
	order, ok := myCache.Get(uid) // тянем из кэша
	var err error
	if !ok {
		order, err = PullFromDB(uid) // если в кэше нет - тянем из БД
		if err != nil {
			slog.Error("err pulling from db", err.Error())
			return Order{}, err
		}
	}
	return order.(Order), nil
}

func main() {
	var err error

	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		if err = srv.Shutdown(context.Background()); err != nil {
			slog.Error("HTTP server Shutdown: ", err.Error())
		}
		close(idleConnsClosed)
	}()

	db, err = pgxpool.New(context.Background(), "host=localhost user=user_1 password=123456 dbname=user_1") // подключение к базе данных
	if err != nil {
		slog.Error("connection err", err.Error())
		return
	}
	defer db.Close()

	_, err = PullFromDB("") // грузим из БД в кэш
	if err != nil {
		slog.Error("err pulling from db", err.Error())
		return
	}

	nc, msgs := CreateStream() // создаем стрим
	defer nc.Close()
	defer msgs.Stop()

	go ServerInit() // запускаем сервер

	<-idleConnsClosed
}
