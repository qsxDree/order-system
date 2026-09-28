package consumer

type TemplateInfo struct {
	Channel  string
	Template string
}

var Templates = map[string]TemplateInfo{
	"order.confirmed":     {Channel: "email", Template: "Your order is confirmed 🎉"},
	"order.cancelled":     {Channel: "email", Template: "Your order was cancelled — any charge has been refunded"},
	"shipment.dispatched": {Channel: "sms", Template: "Your order has shipped"},
	"shipment.delivered":  {Channel: "email", Template: "Your order was delivered"},
}

// GetTemplate returns the channel and template text for the given event type.
func GetTemplate(eventType string) TemplateInfo {
	if t, ok := Templates[eventType]; ok {
		return t
	}
	return TemplateInfo{
		Channel:  "email",
		Template: eventType,
	}
}
