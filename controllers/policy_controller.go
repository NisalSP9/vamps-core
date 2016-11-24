package controllers

import (
	"net/http"
	"strings"

	"encoding/json"
	"errors"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/vedicsoft/vamps-core/commons"
)

const GET_USER_POLICIES = `SELECT policy FROM vs_policies WHERE policyid IN( SELECT policyid from vs_role_policies WHERE
							roleid IN (SELECT roleid FROM vs_user_roles WHERE userid=?))`

const SPLIT_SYMBOL string = "."
const ALL_SYMBOL string = "*"
const ALL_ALL_SYMBOL string = "**"
const (
	FAIL        int = 2
	PASS        int = 1
	NO_DECISION     = 0
)

type Statement struct {
	Effect    string   `json:"effect"`
	Actions   []string `json:"actions"`
	Resources []string `json:"resources"`
}

type VAMPSPolicy struct {
	PolicyID   int         `json:"id"`
	Name       string      `json:"name"`
	Statements []Statement `json:"statements"`
}

func (policy *VAMPSPolicy) evaluate(requestedAction, requestedResource string) int {
	log.Debugf("starting policy %s evaluation for action %s and resource %s", policy.Name, requestedAction,
		requestedResource)
	decision := NO_DECISION
	for _, statement := range policy.Statements {
		if assertAction(statement.Actions, requestedAction) == PASS {
			if assertResource(statement.Resources, requestedResource) == PASS {
				if statement.Effect == "denied" {
					return FAIL
				} else {
					decision = PASS
				}
			}
		}
	}
	return decision
}

func assertAction(policyActions []string, requestedAction string) int {
	k := strings.Split(requestedAction, SPLIT_SYMBOL)
	for _, policyItem := range policyActions {
		checkLength := len(k)
		var matches int
		p := strings.Split(policyItem, SPLIT_SYMBOL)
		n := len(p)
		if n < checkLength && p[n-2] == ALL_ALL_SYMBOL {
			checkLength = n
		}

		for j := 0; j < checkLength; j++ {
			if k[j] != p[j] && p[j] != ALL_SYMBOL && p[j] != ALL_ALL_SYMBOL {
				break
			} else if p[j] == ALL_SYMBOL || k[j] == p[j] {
				matches++
				continue
			} else if p[j] == ALL_ALL_SYMBOL && (k[len(k)-1] == p[n-1] || p[n-1] == ALL_ALL_SYMBOL) {
				log.Debugf("requested action: %s matched with policy action: %s \n", requestedAction, policyItem)
				return PASS
			}
		}
		if matches > 0 && matches == checkLength {
			log.Debugf("requested action: %s matched with policy action: %s \n", requestedAction, policyItem)
			return PASS
		}
	}
	return NO_DECISION
}

func assertResource(policyItems []string, requestedItem string) int {
	k := strings.Split(requestedItem, SPLIT_SYMBOL)
	for _, policyItem := range policyItems {
		checkLength := len(k)
		var matches int
		p := strings.Split(policyItem, SPLIT_SYMBOL)
		n := len(p)
		if n < checkLength && p[n-1] == ALL_ALL_SYMBOL {
			checkLength = n
		}
		for j := 0; j < checkLength; j++ {
			if k[j] != p[j] && p[j] != ALL_SYMBOL && p[j] != ALL_ALL_SYMBOL {

				break
			} else if p[j] == ALL_SYMBOL || k[j] == p[j] {
				matches++
				continue
			} else if p[j] == ALL_ALL_SYMBOL {
				log.Debugf("requested resource: %s matched with policy resource: %s \n", requestedItem, policyItem)
				return PASS
			}
		}

		if matches > 0 && matches == checkLength {
			log.Debugf("requested resource: %s matched with policy resource: %s \n", requestedItem, policyItem)
			return PASS
		}
	}
	return NO_DECISION
}

func (p *VAMPSPolicy) IsValid() bool {
	return false
}

func getUserPolicies(userID int) ([]VAMPSPolicy, error) {
	dbMap := commons.GetDBConnection(commons.PLATFORM_DB)
	var policies []VAMPSPolicy
	var strPolicies []string
	_, err := dbMap.Select(&strPolicies, GET_USER_POLICIES, userID)
	if err != nil {
		errMsg := "error occurred while getting user policies for user:  stack trace: " + err.Error()
		return policies, errors.New(errMsg)
	}
	policies = make([]VAMPSPolicy, len(strPolicies))
	for i, strPolicy := range strPolicies {
		if len(strPolicy) > 0 {
			err = json.Unmarshal([]byte(strPolicy), &policies[i])
			if err != nil {
				return policies, errors.New("error occurred while unmarshalling policy json: " + err.Error())
			}
		} else {
			return policies, errors.New("invalid policy found for userid:" + strconv.Itoa(userID))
		}
	}
	return policies, nil
}

func isAuthorized2(tenantID int, userID int, r *http.Request) (bool, error) {
	resourcePrefix := commons.ServerConfigurations.Prefix

	requestedResource := strings.ToLower(resourcePrefix + strings.Replace(r.URL.Path, "/", ".", -1))
	requestedAction := strings.ToLower(requestedResource + SPLIT_SYMBOL + r.Method)

	userPolicies, err := getUserPolicies(userID)
	if err != nil {
		return false, errors.New("unable to get user policies  stack trace:" + err.Error())
	}
	isAuthorized := false
	for _, userPolicy := range userPolicies {
		result := userPolicy.evaluate(requestedAction, requestedResource)
		if result == FAIL {
			isAuthorized = false
			break
		} else if result == PASS {
			isAuthorized = true
		}
	}
	return isAuthorized, nil
}
