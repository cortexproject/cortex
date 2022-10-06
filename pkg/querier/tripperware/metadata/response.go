package metadata

func (m *Metric) UnmarshalJSON(entry []byte) error {
	var metric map[string]string
	if err := json.Unmarshal(entry, &metric); err != nil {
		return err
	}
	m.Metric = metric
	return nil
}

func (m *Metric) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Metric)
}
