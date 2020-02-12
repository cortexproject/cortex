package alerts

// ToProto con
func ToProto(cfg string, templates map[string]string, user string) (*AlertConfigDesc, error) {
	tmpls := []*TemplateDesc{}
	for fn, body := range templates {
		tmpls = append(tmpls, &TemplateDesc{
			Body:     body,
			Filename: fn,
		})
	}
	return &AlertConfigDesc{
		User:      user,
		RawConfig: cfg,
		Templates: tmpls,
	}, nil
}

// ParseTemplates returns a alertmanager config object
func ParseTemplates(cfg *AlertConfigDesc) map[string]string {
	templates := map[string]string{}
	for _, t := range cfg.Templates {
		templates[t.Filename] = t.Body
	}
	return templates
}
