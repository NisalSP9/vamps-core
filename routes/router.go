package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jcuga/golongpoll"
	"github.com/vedicsoft/vamps-core/controllers"
)

func NewRouter() *mux.Router {
	router := mux.NewRouter().StrictSlash(false)

	manager, _ := golongpoll.StartLongpoll(golongpoll.Options{}) // default options

	// Pass the manager around or create closures and publish:
	manager.Publish("subscription-category", "Some data.  Can be string or any obj convertable to JSON")
	manager.Publish("different-category", "More data")

	// Expose events to browsers
	// See subsection on how to interact with the subscription handler
	http.HandleFunc("/events", manager.SubscriptionHandler)
	for _, route := range ApplicationRoutes {
		var handler http.Handler
		handler = route.HandlerFunc
		if route.Secured {
			if route.CheckAuth {
				handler = controllers.RequireTokenAuthenticationAndAuthorization(handler)
			} else {
				handler = controllers.RequireTokenAuthentication(handler)
			}
		}
		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(handler)
	}

	return router
}
