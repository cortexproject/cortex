package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaveworks/cortex/configs"
	"github.com/weaveworks/cortex/configs/api"
)

// The root page returns 200 OK.
func Test_Root_OK(t *testing.T) {
	setup(t)
	defer cleanup(t)

	w := request(t, "GET", "/", nil)
	assert.Equal(t, http.StatusOK, w.Code)
}

// configs returns 401 to requests without authentication.
func Test_GetUserConfig_Anonymous(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	w := request(t, "GET", fmt.Sprintf("/api/configs/user/%s", subsystem), nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// configs returns 404 if there's never been any configuration for that
// subsystem.
func Test_GetUserConfig_NotFound(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	subsystem := makeSubsystem()
	w := requestAsUser(t, userID, "GET", fmt.Sprintf("/api/configs/user/%s", subsystem), nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// configs returns 401 to requests without authentication.
func Test_PostUserConfig_Anonymous(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	w := request(t, "POST", fmt.Sprintf("/api/configs/user/%s", subsystem), nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// Posting to a configuration sets it so that you can get it again.
func Test_PostUserConfig_CreatesConfig(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	subsystem := makeSubsystem()
	config := makeConfig()
	content := jsonObject(config)
	endpoint := fmt.Sprintf("/api/configs/user/%s", subsystem)
	{
		w := requestAsUser(t, userID, "POST", endpoint, content.Reader(t))
		assert.Equal(t, http.StatusNoContent, w.Code)
	}
	{
		w := requestAsUser(t, userID, "GET", endpoint, nil)
		assert.Equal(t, config, parseConfigView(t, w.Body.Bytes()).Config)
	}
}

// postUserConfig posts a user config.
func postUserConfig(t *testing.T, userID configs.UserID, subsystem configs.Subsystem, config configs.Config) configs.ConfigView {
	endpoint := fmt.Sprintf("/api/configs/user/%s", subsystem)
	w := requestAsUser(t, userID, "POST", endpoint, jsonObject(config).Reader(t))
	require.Equal(t, http.StatusNoContent, w.Code)
	return getUserConfig(t, userID, subsystem)
}

// getUserConfig gets a user config.
func getUserConfig(t *testing.T, userID configs.UserID, subsystem configs.Subsystem) configs.ConfigView {
	endpoint := fmt.Sprintf("/api/configs/user/%s", subsystem)
	w := requestAsUser(t, userID, "GET", endpoint, nil)
	return parseConfigView(t, w.Body.Bytes())
}

// Posting to a configuration sets it so that you can get it again.
func Test_PostUserConfig_UpdatesConfig(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	subsystem := makeSubsystem()
	view1 := postUserConfig(t, userID, subsystem, makeConfig())
	config2 := makeConfig()
	view2 := postUserConfig(t, userID, subsystem, config2)
	assert.True(t, view2.ID > view1.ID, "%v > %v", view2.ID, view1.ID)
	assert.Equal(t, config2, view2.Config)
}

// Different subsystems can have different configurations.
func Test_PostUserConfig_MultipleSubsystems(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	subsystem1 := makeSubsystem()
	subsystem2 := makeSubsystem()
	config1 := postUserConfig(t, userID, subsystem1, makeConfig())
	config2 := postUserConfig(t, userID, subsystem2, makeConfig())
	foundConfig1 := getUserConfig(t, userID, subsystem1)
	assert.Equal(t, config1, foundConfig1)
	foundConfig2 := getUserConfig(t, userID, subsystem2)
	assert.Equal(t, config2, foundConfig2)
	assert.True(t, config2.ID > config1.ID, "%v > %v", config2.ID, config1.ID)
}

// Different users can have different configurations.
func Test_PostUserConfig_MultipleUsers(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID1 := makeUserID()
	userID2 := makeUserID()
	subsystem := makeSubsystem()
	config1 := postUserConfig(t, userID1, subsystem, makeConfig())
	config2 := postUserConfig(t, userID2, subsystem, makeConfig())

	foundConfig1 := getUserConfig(t, userID1, subsystem)
	assert.Equal(t, config1, foundConfig1)
	foundConfig2 := getUserConfig(t, userID2, subsystem)
	assert.Equal(t, config2, foundConfig2)
	assert.True(t, config2.ID > config1.ID, "%v > %v", config2.ID, config1.ID)
}

// postOrgConfig posts an organisation config.
func postOrgConfig(t *testing.T, orgID configs.OrgID, subsystem configs.Subsystem, config configs.Config) configs.ConfigView {
	endpoint := fmt.Sprintf("/api/configs/org/%s", subsystem)
	w := requestAsOrg(t, orgID, "POST", endpoint, jsonObject(config).Reader(t))
	require.Equal(t, http.StatusNoContent, w.Code)
	return getOrgConfig(t, orgID, subsystem)
}

// getOrgConfig gets an organisation config.
func getOrgConfig(t *testing.T, orgID configs.OrgID, subsystem configs.Subsystem) configs.ConfigView {
	endpoint := fmt.Sprintf("/api/configs/org/%s", subsystem)
	w := requestAsOrg(t, orgID, "GET", endpoint, nil)
	return parseConfigView(t, w.Body.Bytes())
}

// configs returns 401 to requests without authentication.
func Test_GetOrgConfig_Anonymous(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	w := request(t, "GET", fmt.Sprintf("/api/configs/org/%s", subsystem), nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// configs returns 404 if there's no such subsystem.
func Test_GetOrgConfig_NotFound(t *testing.T) {
	setup(t)
	defer cleanup(t)

	orgID := makeOrgID()
	subsystem := makeSubsystem()
	w := requestAsOrg(t, orgID, "GET", fmt.Sprintf("/api/configs/org/%s", subsystem), nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// configs returns 401 to requests without authentication.
func Test_PostOrgConfig_Anonymous(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	w := request(t, "POST", fmt.Sprintf("/api/configs/org/%s", subsystem), nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// Posting to a configuration sets it so that you can get it again.
func Test_PostOrgConfig_CreatesConfig(t *testing.T) {
	setup(t)
	defer cleanup(t)

	orgID := makeOrgID()
	subsystem := makeSubsystem()
	config := makeConfig()
	content := jsonObject(config)
	endpoint := fmt.Sprintf("/api/configs/org/%s", subsystem)
	{
		w := requestAsOrg(t, orgID, "POST", endpoint, content.Reader(t))
		assert.Equal(t, http.StatusNoContent, w.Code)
	}
	{
		w := requestAsOrg(t, orgID, "GET", endpoint, nil)
		assert.Equal(t, config, parseConfigView(t, w.Body.Bytes()).Config)
	}
}

// Posting to a configuration sets it so that you can get it again.
func Test_PostOrgConfig_UpdatesConfig(t *testing.T) {
	setup(t)
	defer cleanup(t)

	orgID := makeOrgID()
	subsystem := makeSubsystem()
	view1 := postOrgConfig(t, orgID, subsystem, makeConfig())
	config2 := makeConfig()
	view2 := postOrgConfig(t, orgID, subsystem, config2)
	assert.True(t, view2.ID > view1.ID, "%v > %v", view2.ID, view1.ID)
	assert.Equal(t, config2, view2.Config)
}

// Different subsystems can have different configurations.
func Test_PostOrgConfig_MultipleSubsystems(t *testing.T) {
	setup(t)
	defer cleanup(t)

	orgID := makeOrgID()
	subsystem1 := makeSubsystem()
	subsystem2 := makeSubsystem()
	config1 := postOrgConfig(t, orgID, subsystem1, makeConfig())
	config2 := postOrgConfig(t, orgID, subsystem2, makeConfig())
	foundConfig1 := getOrgConfig(t, orgID, subsystem1)
	assert.Equal(t, config1, foundConfig1)
	foundConfig2 := getOrgConfig(t, orgID, subsystem2)
	assert.Equal(t, config2, foundConfig2)
	assert.True(t, config2.ID > config1.ID, "%v > %v", config2.ID, config1.ID)
}

// Different users can have different configurations.
func Test_PostOrgConfig_MultipleOrgs(t *testing.T) {
	setup(t)
	defer cleanup(t)

	orgID1 := makeOrgID()
	orgID2 := makeOrgID()
	subsystem := makeSubsystem()
	config1 := postOrgConfig(t, orgID1, subsystem, makeConfig())
	config2 := postOrgConfig(t, orgID2, subsystem, makeConfig())

	foundConfig1 := getOrgConfig(t, orgID1, subsystem)
	assert.Equal(t, config1, foundConfig1)
	foundConfig2 := getOrgConfig(t, orgID2, subsystem)
	assert.Equal(t, config2, foundConfig2)
	assert.True(t, config2.ID > config1.ID, "%v > %v", config2.ID, config1.ID)
}

// GetAllOrgConfigs returns an empty list of configs if there aren't any.
func Test_GetAllOrgConfigs_Empty(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	endpoint := fmt.Sprintf("/private/api/configs/org/%s", subsystem)
	w := request(t, "GET", endpoint, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	var found api.OrgConfigsView
	err := json.Unmarshal(w.Body.Bytes(), &found)
	assert.NoError(t, err, "Could not unmarshal JSON")
	assert.Equal(t, api.OrgConfigsView{Configs: map[configs.OrgID]configs.ConfigView{}}, found)
}

// GetAllOrgConfigs returns all created configs.
func Test_GetAllOrgConfigs(t *testing.T) {
	setup(t)
	defer cleanup(t)

	orgID := makeOrgID()
	subsystem := makeSubsystem()
	config := makeConfig()
	view := postOrgConfig(t, orgID, subsystem, config)
	endpoint := fmt.Sprintf("/private/api/configs/org/%s", subsystem)
	w := request(t, "GET", endpoint, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	var found api.OrgConfigsView
	err := json.Unmarshal(w.Body.Bytes(), &found)
	assert.NoError(t, err, "Could not unmarshal JSON")
	assert.Equal(t, api.OrgConfigsView{Configs: map[configs.OrgID]configs.ConfigView{
		orgID: view,
	}}, found)
}

// GetAllOrgConfigs returns the *newest* versions of all created configs.
func Test_GetAllOrgConfigs_Newest(t *testing.T) {
	setup(t)
	defer cleanup(t)

	orgID := makeOrgID()
	subsystem := makeSubsystem()
	postOrgConfig(t, orgID, subsystem, makeConfig())
	postOrgConfig(t, orgID, subsystem, makeConfig())
	lastCreated := postOrgConfig(t, orgID, subsystem, makeConfig())

	endpoint := fmt.Sprintf("/private/api/configs/org/%s", subsystem)
	w := request(t, "GET", endpoint, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	var found api.OrgConfigsView
	err := json.Unmarshal(w.Body.Bytes(), &found)
	assert.NoError(t, err, "Could not unmarshal JSON")
	assert.Equal(t, api.OrgConfigsView{Configs: map[configs.OrgID]configs.ConfigView{
		orgID: lastCreated,
	}}, found)
}

func Test_GetOrgConfigs_IncludesNewerConfigsAndExcludesOlder(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	postOrgConfig(t, makeOrgID(), subsystem, makeConfig())
	config2 := postOrgConfig(t, makeOrgID(), subsystem, makeConfig())
	orgID3 := makeOrgID()
	config3 := postOrgConfig(t, orgID3, subsystem, makeConfig())

	endpoint := fmt.Sprintf("/private/api/configs/org/%s?since=%d", subsystem, config2.ID)
	w := request(t, "GET", endpoint, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	var found api.OrgConfigsView
	err := json.Unmarshal(w.Body.Bytes(), &found)
	assert.NoError(t, err, "Could not unmarshal JSON")
	assert.Equal(t, api.OrgConfigsView{Configs: map[configs.OrgID]configs.ConfigView{
		orgID3: config3,
	}}, found)
}

// GetAllUserConfigs returns an empty list of configs if there aren't any.
func Test_GetAllUserConfigs_Empty(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	endpoint := fmt.Sprintf("/private/api/configs/user/%s", subsystem)
	w := request(t, "GET", endpoint, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	var found api.UserConfigsView
	err := json.Unmarshal(w.Body.Bytes(), &found)
	assert.NoError(t, err, "Could not unmarshal JSON")
	assert.Equal(t, api.UserConfigsView{Configs: map[configs.UserID]configs.ConfigView{}}, found)
}

// GetAllUserConfigs returns all created configs.
func Test_GetAllUserConfigs(t *testing.T) {
	setup(t)
	defer cleanup(t)

	userID := makeUserID()
	subsystem := makeSubsystem()
	config := makeConfig()
	view := postUserConfig(t, userID, subsystem, config)
	endpoint := fmt.Sprintf("/private/api/configs/user/%s", subsystem)
	w := request(t, "GET", endpoint, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	var found api.UserConfigsView
	err := json.Unmarshal(w.Body.Bytes(), &found)
	assert.NoError(t, err, "Could not unmarshal JSON")
	assert.Equal(t, api.UserConfigsView{Configs: map[configs.UserID]configs.ConfigView{
		userID: view,
	}}, found)
}

func Test_GetUserConfigs_IncludesNewerConfigsAndExcludesOlder(t *testing.T) {
	setup(t)
	defer cleanup(t)

	subsystem := makeSubsystem()
	postUserConfig(t, makeUserID(), subsystem, makeConfig())
	config2 := postUserConfig(t, makeUserID(), subsystem, makeConfig())
	userID3 := makeUserID()
	config3 := postUserConfig(t, userID3, subsystem, makeConfig())

	endpoint := fmt.Sprintf("/private/api/configs/user/%s?since=%d", subsystem, config2.ID)
	w := request(t, "GET", endpoint, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	var found api.UserConfigsView
	err := json.Unmarshal(w.Body.Bytes(), &found)
	assert.NoError(t, err, "Could not unmarshal JSON")
	assert.Equal(t, api.UserConfigsView{Configs: map[configs.UserID]configs.ConfigView{
		userID3: config3,
	}}, found)
}
