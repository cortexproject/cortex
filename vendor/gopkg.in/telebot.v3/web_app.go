package telebot

// WebApp represents a parameter of the inline keyboard button
// or the keyboard button used to launch Web App.
type WebApp struct {
	URL string `json:"url"`
}

// WebAppMessage describes an inline message sent by a Web App on behalf of a user.
type WebAppMessage struct {
	InlineMessageID string `json:"inline_message_id"`
}

// WebAppData object represents a data sent from a Web App to the bot
type WebAppData struct {
	Data string `json:"data"`
	Text string `json:"button_text"`
}
