package experimental

import (
	"fmt"
)

type SaslMechanism string

func (sm *SaslMechanism) UnmarshalJSON(b []byte) error {
	s, err := ParseSaslMechanism(string(b))
	if err != nil {
		return err
	}
	*sm = s
	return nil
}

func (sm *SaslMechanism) UnmarshalText(b []byte) error {
	s, err := ParseSaslMechanism(string(b))
	if err != nil {
		return err
	}
	*sm = s
	return nil
}

func (sm *SaslMechanism) String() string {
	return string(*sm)
}

const (
	Plain       SaslMechanism = "PLAIN"
	GSSAPI      SaslMechanism = "GSSAPI"
	ScramSha256 SaslMechanism = "SCRAM-SHA-256"
	ScramSha512 SaslMechanism = "SCRAM-SHA-512"
	OAuthBearer SaslMechanism = "OAUTHBEARER"
)

func ParseSaslMechanism(s string) (SaslMechanism, error) {
	switch s {
	case "PLAIN":
		return Plain, nil
	case "GSSAPI":
		return GSSAPI, nil
	case "SCRAM-SHA-256":
		return ScramSha256, nil
	case "SCRAM-SHA-512":
		return ScramSha512, nil
	case "OAUTHBEARER":
		return OAuthBearer, nil
	default:
		return "", fmt.Errorf("invalid sasl mechanism: %s", s)
	}
}
