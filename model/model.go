package model

type Message struct {
	Address string      `json:"address"`
	Value   interface{} `json:"value"`
}
