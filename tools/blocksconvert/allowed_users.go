package blocksconvert

import (
	"bufio"
	"os"
	"strings"
)

type AllowedUsers map[string]struct{}

var AllowAllUsers = AllowedUsers(nil)

func (a AllowedUsers) AllUsersAllowed() bool {
	return a == nil
}

func (a AllowedUsers) IsAllowed(user string) bool {
	if a == nil {
		return true
	}

	_, ok := a[user]
	return ok
}

func (a AllowedUsers) GetAllowedUsers(users []string) []string {
	if a == nil {
		return users
	}

	allowed := make([]string, 0, len(users))
	for _, u := range users {
		if a.IsAllowed(u) {
			allowed = append(allowed, u)
		}
	}
	return allowed
}

func ParseAllowedUsersFromFile(file string) (AllowedUsers, error) {
	result := map[string]struct{}{}

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer func() { _ = f.Close() }()

	s := bufio.NewScanner(f)
	for s.Scan() {
		result[s.Text()] = struct{}{}
	}
	return result, s.Err()
}

func ParseAllowedUsers(commaSeparated string) AllowedUsers {
	result := map[string]struct{}{}

	us := strings.Split(commaSeparated, ",")
	for _, u := range us {
		u = strings.TrimSpace(u)
		result[u] = struct{}{}
	}

	return result
}
