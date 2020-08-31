package blocksconvert

import (
	"bufio"
	"os"
)

type AllowedUsers map[string]struct{}

var AllowAllUsers = AllowedUsers(nil)

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

func ParseAllowedUsers(file string) (AllowedUsers, error) {
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
