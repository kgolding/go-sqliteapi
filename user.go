package sqliteapi

type SimpleUser struct {
}

func NewUser() *SimpleUser {
	return &SimpleUser{}
}

func (u *SimpleUser) IsAdmin() bool {
	return false
}

func (u *SimpleUser) GetUsername() string {
	return ""
}
