package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/golang/geo/r3"
	dem "github.com/markus-wa/demoinfocs-golang/v2/pkg/demoinfocs"
	common "github.com/markus-wa/demoinfocs-golang/v2/pkg/demoinfocs/common"
	events "github.com/markus-wa/demoinfocs-golang/v2/pkg/demoinfocs/events"
	st "github.com/markus-wa/demoinfocs-golang/v2/pkg/demoinfocs/sendtables"
)

func main() {
	var CTS_PRINTED bool = false
	var TS_PRINTED bool = false
	var END_ROUND bool = false
	//var END_ROUND_OFF bool = false
	// go run DemoScraper.go -demo "./liquid-vs-ence-dust2.dem"
	DemoPath, FileName, rs, roundsTotal := GetArgs()

	f, err := os.Open(DemoPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if strings.Contains(FileName, ".1") {
		FileName = "Parts/" + FileName
	}
	if strings.Contains(FileName, ".2") {
		FileName = "Parts/" + FileName
	}
	outputFile, err := os.OpenFile("./logs/Unclean/"+FileName+".txt", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer outputFile.Close()

	p := dem.NewParser(f)
	defer p.Close()

	header, err := p.ParseHeader()
	if err != nil {
		panic(err)
	}
	_ = header

	/*p.RegisterEventHandler(func(e events.MatchStartedChanged) {
		output = "Match: Game State Changed \n"
		roundFile.WriteString(output)
		outputFile.WriteString(output)

	})*/

	var siteA r3.Vector
	var siteB r3.Vector
	p.RegisterEventHandler(func(events.DataTablesParsed) {
		p.ServerClasses().FindByName("CCSPlayerResource").OnEntityCreated(func(ent st.Entity) {
			siteA = ent.Property("m_bombsiteCenterA").Value().VectorVal
			siteB = ent.Property("m_bombsiteCenterB").Value().VectorVal
		})
	})

	var ctStreak int = 1
	var tStreak int = 1
	p.RegisterEventHandler(func(e events.GameHalfEnded) {
		ctStreak = 1
		tStreak = 1
	})

	p.RegisterEventHandler(func(e events.MatchStart) {
		ctStreak = 1
		tStreak = 1
	})

	p.RegisterEventHandler(func(e events.IsWarmupPeriodChanged) {
		ctStreak = 1
		tStreak = 1
	})

	var roundStartTick int = 0
	var BombPlanted bool = false
	var BombDefused bool = false
	var TimePlanted int = 0
	var site string = ""
	var Winner common.Team
	// Register handler on kill events
	p.RegisterEventHandler(func(e events.Kill) {
		gs := p.GameState()
		roundNo := gs.TeamCounterTerrorists().Score() + gs.TeamTerrorists().Score()

		if !END_ROUND {
			roundNo = roundNo + 1

		}
		if roundNo == 0 {
			roundNo = 1
		}
		var tRoster []*common.Player = gs.TeamTerrorists().Members()
		var tSize int = len(tRoster)
		var ctRoster []*common.Player = gs.TeamCounterTerrorists().Members()
		var ctSize int = len(ctRoster)
		var ctAlive int = 0
		var tAlive int = 0

		if tSize == 5 && ctSize == 5 {
			if !CTS_PRINTED {
				var cts string = p.GameState().TeamCounterTerrorists().ClanName()
				outputCT := fmt.Sprintf("{\"Team\":\"%s\", \"Players\": [\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"]}\n", cts, ctRoster[0], ctRoster[1], ctRoster[2], ctRoster[3], ctRoster[4])
				outputFile.WriteString(outputCT)
				CTS_PRINTED = true
			}
			if !TS_PRINTED {
				var ts string = p.GameState().TeamTerrorists().ClanName()
				outputT := fmt.Sprintf("{\"Team\":\"%s\", \"Players\": [\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"]}\n", ts, tRoster[0], tRoster[1], tRoster[2], tRoster[3], tRoster[4])
				outputFile.WriteString(outputT)
				TS_PRINTED = true
			}
			for i := 0; i < 5; i++ {
				if tRoster[i].IsAlive() {
					tAlive += 1
				}
				if ctRoster[i].IsAlive() {
					ctAlive += 1
				}
			}
			if !gs.IsWarmupPeriod() {
				tick := gs.IngameTick()
				var KillerX, KillerY, KillerZ, KillerXvel, KillerYvel, KillerZvel, KillerPitch, KillerYaw float32 = 0, 0, 0, 0, 0, 0, 0, 0
				var VictimX, VictimY, VictimZ, VictimXvel, VictimYvel, VictimZvel, VictimPitch, VictimYaw float32 = 0, 0, 0, 0, 0, 0, 0, 0
				var KillerArmour, KillerHealth, VictimArmour, VictimHealth, KillerFlash, VictimFlash int = 0, 0, 0, 0, 0, 0
				var KillerHelmet, VictimHelmet, Spotted bool = false, false, false
				planting_defusing := e.Victim.IsPlanting || e.Victim.IsDefusing
				if e.Killer != (nil) {

					KillerX = float32(e.Killer.Position().X)
					KillerY = float32(e.Killer.Position().Y)
					KillerZ = float32(e.Killer.PositionEyes().Z)
					KillerXvel = float32(e.Killer.Velocity().X)
					KillerYvel = float32(e.Killer.Velocity().Y)
					KillerZvel = float32(e.Killer.Velocity().Z)

					KillerPitch = e.Killer.ViewDirectionY()
					if KillerPitch > 90 {
						KillerPitch -= 360
					}
					KillerPitch = -1 * KillerPitch
					KillerYaw = e.Killer.ViewDirectionX()

					VictimX = float32(e.Victim.Position().X)
					VictimY = float32(e.Victim.Position().Y)
					VictimZ = float32(e.Victim.PositionEyes().Z)
					VictimXvel = float32(e.Victim.Velocity().X)
					VictimYvel = float32(e.Victim.Velocity().Y)
					VictimZvel = float32(e.Victim.Velocity().Z)

					VictimPitch = e.Victim.ViewDirectionY() * -1
					if VictimPitch > 90 {
						VictimPitch -= 360
					}
					VictimPitch = -1 * VictimPitch
					VictimYaw = e.Victim.ViewDirectionX()

					KillerArmour = e.Killer.Armor()
					KillerHealth = e.Killer.Health()

					VictimArmour = e.Victim.Armor()
					VictimHealth = e.Victim.Health()

					KillerHelmet = e.Killer.HasHelmet()
					VictimHelmet = e.Victim.HasHelmet()

					KillerFlash = int(e.Killer.FlashDurationTimeRemaining().Milliseconds())
					VictimFlash = int(e.Victim.FlashDurationTimeRemaining().Milliseconds())

					Spotted = e.Victim.HasSpotted(e.Killer)
				}
				if e.Weapon.String() == "C4" {
					roundNo = roundNo + 1
				}
				if e.Victim.Team == 3 {
					output := fmt.Sprintf("{\"Round\":\"%d\", \"Tick\":\"%d\", \"teamMembersAlive\":\"%d\", \"opponentsAlive\":\"%d\", \"Kill\":\"%s\", \"Death\": \"%s\", \"Weapon\": \"%s\", \"Headshot\": \"%t\", \"KillerHealth\": \"%d\", \"KillerArmor\": \"%d\", \"KillerHelmet\": \"%t\", \"VictimHealth\": \"%d\", \"VictimArmor\": \"%d\", \"VictimHelmet\": \"%t\", \"KillerX\": \"%f\", \"KillerY\": \"%f\", \"KillerZ\": \"%f\", \"KillerPitch\": \"%f\", \"KillerYaw\": \"%f\", \"VictimX\": \"%f\", \"VictimY\": \"%f\", \"VictimZ\": \"%f\", \"VictimPitch\": \"%f\", \"VictimYaw\": \"%f\", \"KillerFlashDuration\": \"%d\", \"VictimFlashDuration\": \"%d\", \"VictimPlantingOrDefusing\": \"%t\", \"VictimWeapon\": \"%s\", \"VictimReloading\": \"%t\", \"VictimSpottedKiller\": \"%t\", \"KillerXvel\": \"%f\", \"KillerYvel\": \"%f\", \"KillerZvel\": \"%f\", \"VictimXvel\": \"%f\", \"VictimYvel\": \"%f\", \"VictimZvel\": \"%f\"}\n", roundNo, tick-roundStartTick, tAlive, ctAlive, e.Killer, e.Victim, e.Weapon, e.IsHeadshot, KillerHealth, KillerArmour, KillerHelmet, VictimHealth, VictimArmour, VictimHelmet, KillerX, KillerY, KillerZ, KillerPitch, KillerYaw, VictimX, VictimY, VictimZ, VictimPitch, VictimYaw, KillerFlash, VictimFlash, planting_defusing, e.Victim.ActiveWeapon(), e.Victim.IsReloading, Spotted, KillerXvel, KillerYvel, KillerZvel, VictimXvel, VictimYvel, VictimZvel)
					outputFile.WriteString(output)
				}
				if e.Victim.Team == 2 {
					output := fmt.Sprintf("{\"Round\":\"%d\", \"Tick\":\"%d\", \"teamMembersAlive\":\"%d\", \"opponentsAlive\":\"%d\", \"Kill\":\"%s\", \"Death\": \"%s\", \"Weapon\": \"%s\", \"Headshot\": \"%t\", \"KillerHealth\": \"%d\", \"KillerArmor\": \"%d\", \"KillerHelmet\": \"%t\", \"VictimHealth\": \"%d\", \"VictimArmor\": \"%d\", \"VictimHelmet\": \"%t\", \"KillerX\": \"%f\", \"KillerY\": \"%f\", \"KillerZ\": \"%f\", \"KillerPitch\": \"%f\", \"KillerYaw\": \"%f\", \"VictimX\": \"%f\", \"VictimY\": \"%f\", \"VictimZ\": \"%f\", \"VictimPitch\": \"%f\", \"VictimYaw\": \"%f\", \"KillerFlashDuration\": \"%d\", \"VictimFlashDuration\": \"%d\", \"VictimPlantingOrDefusing\": \"%t\", \"VictimWeapon\": \"%s\", \"VictimReloading\": \"%t\", \"VictimSpottedKiller\": \"%t\", \"KillerXvel\": \"%f\", \"KillerYvel\": \"%f\", \"KillerZvel\": \"%f\", \"VictimXvel\": \"%f\", \"VictimYvel\": \"%f\", \"VictimZvel\": \"%f\"}\n", roundNo, tick-roundStartTick, ctAlive, tAlive, e.Killer, e.Victim, e.Weapon, e.IsHeadshot, KillerHealth, KillerArmour, KillerHelmet, VictimHealth, VictimArmour, VictimHelmet, KillerX, KillerY, KillerZ, KillerPitch, KillerYaw, VictimX, VictimY, VictimZ, VictimPitch, VictimYaw, KillerFlash, VictimFlash, planting_defusing, e.Victim.ActiveWeapon(), e.Victim.IsReloading, Spotted, KillerXvel, KillerYvel, KillerZvel, VictimXvel, VictimYvel, VictimZvel)
					outputFile.WriteString(output)

				}
			}
		}

	})

	p.RegisterEventHandler(func(e events.RoundStart) {
		END_ROUND = false
		//END_ROUND_OFF = false
		BombPlanted = false
		BombDefused = false
		TimePlanted = 0
		site = ""

	})
	p.RegisterEventHandler(func(e events.RoundFreezetimeEnd) {
		gs := p.GameState()
		roundStartTick = gs.IngameTick()
		WinProb(gs, roundStartTick, BombPlanted, TimePlanted, outputFile, site, BombDefused, END_ROUND, siteA, siteB, "None", "None", 0, rs)

	})

	p.RegisterEventHandler(func(e events.BombPlanted) {
		gs := p.GameState()
		BombPlanted = true
		TimePlanted = gs.IngameTick()
		var ctRoster []*common.Player = gs.TeamCounterTerrorists().Members()
		var ctSize int = len(ctRoster)

		if e.Site == events.BombsiteA {
			site = "A"
		}
		if e.Site == events.BombsiteB {
			site = "B"
		}

		if ctSize > 0 {
			WinProb(gs, roundStartTick, BombPlanted, TimePlanted, outputFile, site, BombDefused, END_ROUND, siteA, siteB, e.Player.Name, "None", 0, rs)
		}

	})

	p.RegisterEventHandler(func(e events.BombDefused) {
		END_ROUND = true

	})

	p.RegisterEventHandler(func(e events.PlayerHurt) {
		gs := p.GameState()
		if e.Attacker != (nil) {
			Attacker := e.Attacker.Name
			Victim := e.Player.Name
			DMG := e.HealthDamageTaken
			WinProb(gs, roundStartTick, BombPlanted, TimePlanted, outputFile, site, BombDefused, END_ROUND, siteA, siteB, Attacker, Victim, DMG, rs)
		}
	})

	p.RegisterEventHandler(func(e events.Footstep) {
		gs := p.GameState()
		if rs == "1" {
			WinProb(gs, roundStartTick, BombPlanted, TimePlanted, outputFile, site, BombDefused, END_ROUND, siteA, siteB, "None", "None", 0, rs)
		}

	})

	var ctMoney int = 0
	var tMoney int = 0
	var RZN events.RoundEndReason
	p.RegisterEventHandler(func(e events.RoundEnd) {
		END_ROUND = true
		RZN = e.Reason
		Winner = e.Winner
		gs := p.GameState()
		roundNo := gs.TeamCounterTerrorists().Score() + gs.TeamTerrorists().Score() + 1
		if fmt.Sprintf("%d", roundNo) == roundsTotal {
			var tRoster []*common.Player = gs.TeamTerrorists().Members()
			var tSize int = len(tRoster)
			var ctRoster []*common.Player = gs.TeamCounterTerrorists().Members()
			var ctSize int = len(ctRoster)
			var ctAlive int = 0
			var tAlive int = 0
			var ctValue int = 0
			var tValue int = 0
			var ctValueEnd int = 0
			var tValueEnd int = 0
			if tSize == 5 && ctSize == 5 {
				for i := 0; i < 5; i++ {
					tValue += tRoster[i].EquipmentValueFreezeTimeEnd()
					ctValue += ctRoster[i].EquipmentValueFreezeTimeEnd()
					if tRoster[i].IsAlive() {
						tAlive += 1
						tValueEnd += tRoster[i].EquipmentValueCurrent()
					}
					if ctRoster[i].IsAlive() {
						ctAlive += 1
						ctValueEnd += ctRoster[i].EquipmentValueCurrent()
					}
				}
			}
			if tAlive > ctAlive {
				Winner = common.TeamTerrorists
			} else {
				Winner = common.TeamCounterTerrorists
			}

			switch Winner {
			case common.TeamTerrorists:
				var members []*common.Player = gs.TeamCounterTerrorists().Members()
				for _, player := range members {
					if player.IsAlive() && roundNo != 15 && roundNo != 30 {
						tick := gs.IngameTick()
						output := fmt.Sprintf("{\"Round\":\"%d\", \"Tick\":\"%d\", \"teamMembersAlive\":\"%d\", \"opponentsAlive\":\"%d\", \"Save\":\"%s\", \"SavedValue\":\"%d\"}\n", roundNo, tick-roundStartTick, ctAlive, tAlive, player, player.EquipmentValueCurrent())
						outputFile.WriteString(output)
					}
				}

			case common.TeamCounterTerrorists:
				var members []*common.Player = gs.TeamTerrorists().Members()
				for _, player := range members {
					if player.IsAlive() && roundNo != 15 && roundNo != 30 {
						tick := gs.IngameTick()
						output := fmt.Sprintf("{\"Round\":\"%d\", \"Tick\":\"%d\", \"teamMembersAlive\":\"%d\", \"opponentsAlive\":\"%d\", \"Save\":\"%s\", \"SavedValue\":\"%d\"}\n", roundNo, tick-roundStartTick, tAlive, ctAlive, player, player.EquipmentValueCurrent())
						outputFile.WriteString(output)
					}
				}
			}

			if roundNo == 0 {
				ctStreak = 1
				tStreak = 1
			}
			switch Winner {
			case common.TeamTerrorists:

				if RZN != 1 && RZN != 7 && RZN != 12 {
					ctAlive -= 1
					if ctAlive == 0 {
						ctValueEnd = 0
					}
				}
				output := fmt.Sprintf("{\"Round\":\"%d\", \"WinnerSide\":\"%s\", \"Winner\":\"%s\", \"Loser\":\"%s\", \"WinnerScore\": \"%d\", \"LoserScore\": \"%d\", \"WinnerValue\": \"%d\", \"LoserValue\": \"%d\", \"WinnerAlive\": \"%d\", \"LoserAlive\": \"%d\", \"WinnerSaved\": \"%d\", \"LoserSaved\": \"%d\", \"WinnerMoney\": \"%d\", \"LoserMoney\": \"%d\", \"WinnerStreak\": \"%d\", \"LoserStreak\": \"%d\"}\n", roundNo, "T", gs.TeamTerrorists().ClanName(), gs.TeamCounterTerrorists().ClanName(), gs.TeamTerrorists().Score()+1, gs.TeamCounterTerrorists().Score(), tValue, ctValue, tAlive, ctAlive, tValueEnd, ctValueEnd, tMoney, ctMoney, tStreak, ctStreak)
				outputFile.WriteString(output)
				if tStreak > 0 {
					tStreak -= 1
				}
				if ctStreak < 4 {
					ctStreak += 1
				}
			case common.TeamCounterTerrorists:

				if RZN != 1 && RZN != 7 && RZN != 12 {
					tAlive -= 1
					if tAlive == 0 {
						tValueEnd = 0
					}

				}
				output := fmt.Sprintf("{\"Round\":\"%d\", \"WinnerSide\":\"%s\", \"Winner\":\"%s\", \"Loser\":\"%s\", \"WinnerScore\": \"%d\", \"LoserScore\": \"%d\", \"WinnerValue\": \"%d\", \"LoserValue\": \"%d\", \"WinnerAlive\": \"%d\", \"LoserAlive\": \"%d\", \"WinnerSaved\": \"%d\", \"LoserSaved\": \"%d\", \"WinnerMoney\": \"%d\", \"LoserMoney\": \"%d\", \"WinnerStreak\": \"%d\", \"LoserStreak\": \"%d\"}\n", roundNo, "CT", gs.TeamCounterTerrorists().ClanName(), gs.TeamTerrorists().ClanName(), gs.TeamCounterTerrorists().Score()+1, gs.TeamTerrorists().Score(), ctValue, tValue, ctAlive, tAlive, ctValueEnd, tValueEnd, ctMoney, tMoney, ctStreak, tStreak)
				outputFile.WriteString(output)
				if ctStreak > 0 {
					ctStreak -= 1
				}
				if tStreak < 4 {
					tStreak += 1
				}
			default:
				// Probably match medic or something similar
				output := "Round: NOT_LIVE\n"
				outputFile.WriteString(output)
			}
			ctMoney = 0
			tMoney = 0
			if tSize == 5 && ctSize == 5 {
				for i := 0; i < 5; i++ {
					ctMoney += ctRoster[i].Money()
					tMoney += tRoster[i].Money()
				}
			}
		}
	})

	p.RegisterEventHandler(func(e events.RoundEndOfficial) {
		//END_ROUND_OFF = true
		BombPlanted = false
		gs := p.GameState()
		roundNo := gs.TeamCounterTerrorists().Score() + gs.TeamTerrorists().Score()
		var tRoster []*common.Player = gs.TeamTerrorists().Members()
		var tSize int = len(tRoster)
		var ctRoster []*common.Player = gs.TeamCounterTerrorists().Members()
		var ctSize int = len(ctRoster)
		var ctAlive int = 0
		var tAlive int = 0
		var ctValue int = 0
		var tValue int = 0
		var ctValueEnd int = 0
		var tValueEnd int = 0
		if tSize == 5 && ctSize == 5 {
			for i := 0; i < 5; i++ {
				tValue += tRoster[i].EquipmentValueFreezeTimeEnd()
				ctValue += ctRoster[i].EquipmentValueFreezeTimeEnd()
				if tRoster[i].IsAlive() {
					tAlive += 1
					tValueEnd += tRoster[i].EquipmentValueCurrent()
				}
				if ctRoster[i].IsAlive() {
					ctAlive += 1
					ctValueEnd += ctRoster[i].EquipmentValueCurrent()
				}
			}
		}
		if tAlive > ctAlive {
			Winner = common.TeamTerrorists
		} else {
			Winner = common.TeamCounterTerrorists
		}

		switch Winner {
		case common.TeamTerrorists:
			var members []*common.Player = gs.TeamCounterTerrorists().Members()
			for _, player := range members {
				if player.IsAlive() && roundNo != 15 && roundNo != 30 {
					tick := gs.IngameTick()
					output := fmt.Sprintf("{\"Round\":\"%d\", \"Tick\":\"%d\", \"teamMembersAlive\":\"%d\", \"opponentsAlive\":\"%d\", \"Save\":\"%s\", \"SavedValue\":\"%d\"}\n", roundNo, tick-roundStartTick, ctAlive, tAlive, player, player.EquipmentValueCurrent())
					outputFile.WriteString(output)
				}
			}

		case common.TeamCounterTerrorists:
			var members []*common.Player = gs.TeamTerrorists().Members()
			for _, player := range members {
				if player.IsAlive() && roundNo != 15 && roundNo != 30 {
					tick := gs.IngameTick()
					output := fmt.Sprintf("{\"Round\":\"%d\", \"Tick\":\"%d\", \"teamMembersAlive\":\"%d\", \"opponentsAlive\":\"%d\", \"Save\":\"%s\", \"SavedValue\":\"%d\"}\n", roundNo, tick-roundStartTick, tAlive, ctAlive, player, player.EquipmentValueCurrent())
					outputFile.WriteString(output)
				}
			}
		}

		if roundNo == 0 {
			ctStreak = 1
			tStreak = 1
		}
		switch Winner {
		case common.TeamTerrorists:

			if RZN != 1 && RZN != 7 && RZN != 12 {
				ctAlive -= 1
				if ctAlive == 0 {
					ctValueEnd = 0
				}
			}
			output := fmt.Sprintf("{\"Round\":\"%d\", \"WinnerSide\":\"%s\", \"Winner\":\"%s\", \"Loser\":\"%s\", \"WinnerScore\": \"%d\", \"LoserScore\": \"%d\", \"WinnerValue\": \"%d\", \"LoserValue\": \"%d\", \"WinnerAlive\": \"%d\", \"LoserAlive\": \"%d\", \"WinnerSaved\": \"%d\", \"LoserSaved\": \"%d\", \"WinnerMoney\": \"%d\", \"LoserMoney\": \"%d\", \"WinnerStreak\": \"%d\", \"LoserStreak\": \"%d\"}\n", roundNo, "T", gs.TeamTerrorists().ClanName(), gs.TeamCounterTerrorists().ClanName(), gs.TeamTerrorists().Score(), gs.TeamCounterTerrorists().Score(), tValue, ctValue, tAlive, ctAlive, tValueEnd, ctValueEnd, tMoney, ctMoney, tStreak, ctStreak)
			outputFile.WriteString(output)
			if tStreak > 0 {
				tStreak -= 1
			}
			if ctStreak < 4 {
				ctStreak += 1
			}
		case common.TeamCounterTerrorists:

			if RZN != 1 && RZN != 7 && RZN != 12 {
				tAlive -= 1
				if tAlive == 0 {
					tValueEnd = 0
				}

			}
			output := fmt.Sprintf("{\"Round\":\"%d\", \"WinnerSide\":\"%s\", \"Winner\":\"%s\", \"Loser\":\"%s\", \"WinnerScore\": \"%d\", \"LoserScore\": \"%d\", \"WinnerValue\": \"%d\", \"LoserValue\": \"%d\", \"WinnerAlive\": \"%d\", \"LoserAlive\": \"%d\", \"WinnerSaved\": \"%d\", \"LoserSaved\": \"%d\", \"WinnerMoney\": \"%d\", \"LoserMoney\": \"%d\", \"WinnerStreak\": \"%d\", \"LoserStreak\": \"%d\"}\n", roundNo, "CT", gs.TeamCounterTerrorists().ClanName(), gs.TeamTerrorists().ClanName(), gs.TeamCounterTerrorists().Score(), gs.TeamTerrorists().Score(), ctValue, tValue, ctAlive, tAlive, ctValueEnd, tValueEnd, ctMoney, tMoney, ctStreak, tStreak)
			outputFile.WriteString(output)
			if ctStreak > 0 {
				ctStreak -= 1
			}
			if tStreak < 4 {
				tStreak += 1
			}
		default:
			// Probably match medic or something similar
			output := "Round: NOT_LIVE\n"
			outputFile.WriteString(output)
		}
		ctMoney = 0
		tMoney = 0
		if tSize == 5 && ctSize == 5 {
			for i := 0; i < 5; i++ {
				ctMoney += ctRoster[i].Money()
				tMoney += tRoster[i].Money()
			}
		}

	})
	// Parse to end
	err = p.ParseToEnd()
	if err != nil {
		fmt.Print(FileName + "\n")
		fmt.Print(p.CurrentFrame())
		fmt.Print("\n")
		panic(err)
	}

}

func GetArgs() (string, string, string, string) {
	fl := new(flag.FlagSet)

	demPathPtr := fl.String("demo", "", "Demo file `path`")
	FileNamePtr := fl.String("filename", "", "filename `path`")
	rsPtr := fl.String("rs", "", "rs `path`")
	roundsPtr := fl.String("roundsTotal", "", "roundsTotal `path`")
	err := fl.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}

	demPath := *demPathPtr
	FileName := *FileNamePtr
	rs := *rsPtr
	roundsTotal := *roundsPtr
	return demPath, FileName, rs, roundsTotal
}

func WinProb(gs dem.GameState, roundStartTick int, BombPlanted bool, TimePlanted int, outputFile *os.File, site string, BombDefused bool, END_ROUND bool, siteA r3.Vector, siteB r3.Vector, attacker string, victim string, DMG int, rs string) {
	roundNo := gs.TeamCounterTerrorists().Score() + gs.TeamTerrorists().Score()

	if !END_ROUND {
		roundNo += 1
	}
	if roundNo == 0 {
		roundNo = 1
	}
	var tRoster []*common.Player = gs.TeamTerrorists().Members()
	var tSize int = len(tRoster)
	var ctRoster []*common.Player = gs.TeamCounterTerrorists().Members()
	var ctSize int = len(ctRoster)
	var ctAlive int = 0
	var tAlive int = 0
	var ctValueEnd int = 0
	var tValueEnd int = 0
	var ctHP int = 0
	var tHP int = 0
	var ctDistAmin float64 = 100000
	var tDistAmin float64 = 100000
	var ctDistBmin float64 = 100000
	var tDistBmin float64 = 100000
	if tSize == 5 && ctSize == 5 {
		for i := 0; i < 5; i++ {

			if tRoster[i].IsAlive() {
				tAlive += 1
				tValueEnd += tRoster[i].EquipmentValueCurrent()
				tHP += tRoster[i].Health()
				tRoster[i].Position()
				tDistA := math.Sqrt(math.Pow((tRoster[i].Position().X-siteA.X), 2) + math.Pow((tRoster[i].Position().Y-siteA.Y), 2))
				tDistB := math.Sqrt(math.Pow((tRoster[i].Position().X-siteB.X), 2) + math.Pow((tRoster[i].Position().Y-siteB.Y), 2))
				if tDistA < tDistAmin {
					tDistAmin = tDistA
				}
				if tDistB < tDistBmin {
					tDistBmin = tDistB
				}

			}
			if ctRoster[i].IsAlive() {
				ctAlive += 1
				ctValueEnd += ctRoster[i].EquipmentValueCurrent()
				ctHP += ctRoster[i].Health()
				ctDistA := math.Sqrt(math.Pow((ctRoster[i].Position().X-siteA.X), 2) + math.Pow((ctRoster[i].Position().Y-siteA.Y), 2))
				ctDistB := math.Sqrt(math.Pow((ctRoster[i].Position().X-siteB.X), 2) + math.Pow((ctRoster[i].Position().Y-siteB.Y), 2))
				if ctDistA < ctDistAmin {
					ctDistAmin = ctDistA
				}
				if ctDistB < ctDistBmin {
					ctDistBmin = ctDistB
				}
			}
		}
	}
	tick := gs.IngameTick() - roundStartTick
	TimeSincePlanted := 0
	if BombPlanted {
		TimeSincePlanted = (gs.IngameTick() - TimePlanted)
	}
	if ctAlive > 0 && tAlive > 0 {
		output := fmt.Sprintf("{\"Round\":\"%d\", \"Tick\":\"%d\", \"CT\":\"%s\", \"T\":\"%s\", \"CTalive\":\"%d\", \"Talive\":\"%d\", \"CTdistA\":\"%f\", \"TdistA\":\"%f\", \"CTdistB\":\"%f\", \"TdistB\":\"%f\", \"CTvalue\":\"%d\", \"Tvalue\":\"%d\", \"CThp\":\"%d\", \"Thp\":\"%d\", \"BombPlanted\":\"%t\", \"TimeSincePlant\":\"%d\", \"PlantSite\":\"%s\", \"Attacker\":\"%s\", \"Victim\":\"%s\", \"Damage\":\"%d\"}\n", roundNo, tick, gs.TeamCounterTerrorists().ClanName(), gs.TeamTerrorists().ClanName(), ctAlive, tAlive, ctDistAmin, tDistAmin, ctDistBmin, tDistBmin, ctValueEnd, tValueEnd, ctHP, tHP, BombPlanted, TimeSincePlanted, site, attacker, victim, DMG)
		//_ = output
		outputFile.WriteString(output)
	}
}
