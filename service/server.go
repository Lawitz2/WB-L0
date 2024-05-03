package main

import (
	"html/template"
	"log"
	"log/slog"
	"net/http"
	"strings"
)

var srv http.Server

func ServerInit() {
	var tpl, tplNotFound *template.Template

	tpl, _ = tpl.ParseGlob("dynamic/index.html")
	tplNotFound, _ = tplNotFound.ParseGlob("dynamic/notfound.html")

	MainHandle := func(w http.ResponseWriter, r *http.Request) {
		v := r.FormValue("order")
		v = strings.Trim(v, " ")
		if len(v) == 0 {
			slog.Error("empty input")
			tplNotFound.ExecuteTemplate(w, "notfound.html", v)
			return
		}
		box, err := RetrieveOrder(v)
		if err != nil {
			slog.Error("couldn't retrieve order ", err.Error())
			tplNotFound.ExecuteTemplate(w, "notfound.html", v)
			return
		}
		if box.OrderUid != v {
			tplNotFound.ExecuteTemplate(w, "notfound.html", v)
			return
		}
		tpl.ExecuteTemplate(w, "index.html", box)
	}

	srv = http.Server{
		Addr: ":8081",
	}

	http.HandleFunc("/", MainHandle)

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
