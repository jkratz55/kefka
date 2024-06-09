package experimental

import (
	"fmt"
)

type SecurityProtocol string

func (sp *SecurityProtocol) UnmarshalJSON(b []byte) error {
	s, err := ParseSecurityProtocol(string(b))
	if err != nil {
		return err
	}
	*sp = s
	return nil
}

func (sp *SecurityProtocol) UnmarshalText(b []byte) error {
	s, err := ParseSecurityProtocol(string(b))
	if err != nil {
		return err
	}
	*sp = s
	return nil
}

func (sp *SecurityProtocol) String() string {
	return string(*sp)
}

const (
	Plaintext     SecurityProtocol = "plaintext"
	Ssl           SecurityProtocol = "ssl"
	SaslPlaintext SecurityProtocol = "sasl_plaintext"
	SaslSsl       SecurityProtocol = "sasl_ssl"
)

func ParseSecurityProtocol(s string) (SecurityProtocol, error) {
	switch s {
	case "plaintext":
		return Plaintext, nil
	case "ssl":
		return Ssl, nil
	case "sasl_plaintext":
		return SaslPlaintext, nil
	case "sasl_ssl":
		return SaslSsl, nil
	default:
		return "", fmt.Errorf("invalid security protocol: %s", s)
	}
}
