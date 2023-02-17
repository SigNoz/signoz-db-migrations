package main

import (
	"fmt"

	"go.uber.org/zap"
)

func GetStoredRules() ([]StoredRule, error) {

	rules := []StoredRule{}

	query := fmt.Sprintf("SELECT id, updated_at, data FROM rules")

	err := db.Select(&rules, query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, err
	}

	return rules, nil
}

func migrateRule(rule StoredRule) error {

	var err error

	// do something with the rule

	return err
}

func migrateRules() error {

	rules, err := GetStoredRules()

	if err != nil {
		return err
	}

	for _, rule := range rules {
		err := migrateRule(rule)
		if err != nil {
			return err
		}
	}

	return nil
}
