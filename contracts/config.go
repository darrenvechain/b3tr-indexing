package contracts

import (
	"bytes"
	"embed"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

//go:embed abis
var FS embed.FS

var B3trAddress = common.HexToAddress("0x5ef79995FE8a89e0812330E4378eB2660ceDe699")
var B3trGovernorAddress = common.HexToAddress("0x1c65C25fABe2fc1bCb82f253fA0C916a322f777C")
var EmissionsAddress = common.HexToAddress("0xDf94739bd169C84fe6478D8420Bb807F1f47b135")
var GalaxyMemberAddress = common.HexToAddress("0x93B8cD34A7Fc4f53271b9011161F7A2B5fEA9D1F")
var TreasuryAddress = common.HexToAddress("0xD5903BCc66e439c753e525F8AF2FeC7be2429593")
var Vot3Address = common.HexToAddress("0x76Ca782B59C74d088C7D2Cce2f211BC00836c602")
var X2eAppsAddress = common.HexToAddress("0x8392B7CCc763dB03b47afcD8E8f5e24F9cf0554D")
var X2eRewardsAddress = common.HexToAddress("0x6Bee7DDab6c99d5B2Af0554EaEA484CE18F52631")
var XAllocationPoolAddress = common.HexToAddress("0x4191776F05f4bE4848d3f4d587345078B439C7d3")
var XAllocationVotingAddress = common.HexToAddress("0x89A00Bb0947a30FF95BEeF77a66AEdE3842Fe5B7")

var B3trABI = mustParseABI("b3tr.json")
var B3trGovernorABI = mustParseABI("governor.json")
var EmissionsABI = mustParseABI("emissions.json")
var GalaxyMemberABI = mustParseABI("galaxy-member.json")
var TreasuryABI = mustParseABI("treasury.json")
var Vot3ABI = mustParseABI("vot3.json")
var X2eAppsABI = mustParseABI("x2e_apps.json")
var X2eRewardsABI = mustParseABI("x2e_rewards_pool.json")
var XAllocationPoolABI = mustParseABI("x_allocation_pool.json")
var XAllocationVotingABI = mustParseABI("x_allocation_voting.json")

func mustParseABI(path string) abi.ABI {
	content, err := FS.ReadFile("abis/" + path)
	reader := bytes.NewReader(content)
	contract, err := abi.JSON(reader)
	if err != nil {
		panic(err)
	}
	return contract
}
