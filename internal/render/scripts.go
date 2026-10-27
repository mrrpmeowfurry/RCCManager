package render

import (
	"strconv"
	"strings"
)

const (
	avatarSize     = 420
	headshotSize   = 150
	thumbW, thumbH = 768, 432
	itemSize       = 420
)

func luaString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch ch := s[i]; ch {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteByte(ch)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case 0:
			b.WriteString(`\0`)
		default:
			b.WriteByte(ch)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func luaBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func preamble(siteURL string) string {
	return `game:GetService("ContentProvider"):SetBaseUrl(` + luaString(siteURL+"/") + `)` + "\n" +
		`game:GetService("ScriptContext").ScriptsDisabled = true` + "\n"
}

// a character that failed to load crashes rcc on Click, so bail with an error before that
const loadCharacter = `local ok, err = pcall(function() player:LoadCharacter(false) end)
if not ok then error("character did not load: " .. tostring(err)) end
local character = player.Character
local head = character and character:FindFirstChild("Head")
if not head then error("character has no head, appearance url probably failed") end
`

func avatarSetup(b *strings.Builder, siteURL string, userID int64, appearanceURL string, r15 bool) {
	b.WriteString(preamble(siteURL))
	if r15 {
		b.WriteString(`pcall(function() game.ForceR15 = true end)` + "\n")
	}
	b.WriteString(`local player = game:GetService("Players"):CreateLocalPlayer(` + strconv.FormatInt(userID, 10) + `)` + "\n")
	b.WriteString(`player.CharacterAppearance = ` + luaString(appearanceURL) + "\n")
	b.WriteString(`player.CanLoadCharacterAppearance = true` + "\n")
	b.WriteString(loadCharacter)
}

// headshot = a ThumbnailCamera parented in front of the head first
func avatarScript(siteURL string, userID int64, appearanceURL string, w, h int, headshot bool, r15 bool) string {
	var b strings.Builder
	avatarSetup(&b, siteURL, userID, appearanceURL, r15)
	if headshot {
		b.WriteString(`local cam = Instance.new("Camera")` + "\n")
		b.WriteString(`cam.Name = "ThumbnailCamera"` + "\n")
		b.WriteString(`cam.CameraType = Enum.CameraType.Scriptable` + "\n")
		b.WriteString(`local hp = head.Position` + "\n")
		b.WriteString(`cam.CoordinateFrame = CFrame.new(hp + head.CFrame.lookVector * 2.6 + Vector3.new(0, 0.45, 0), hp + Vector3.new(0, 0.1, 0))` + "\n")
		b.WriteString(`cam.Parent = game.Workspace:GetChildren()[1]` + "\n")
	}
	b.WriteString(`return game:GetService("ThumbnailGenerator"):Click("PNG", ` + strconv.Itoa(w) + `, ` + strconv.Itoa(h) + `, true, false)`)
	return b.String()
}

// OBJ makes Click hand back the scene as json, obj + mtl + textures inside
func avatar3DScript(siteURL string, userID int64, appearanceURL string, r15 bool) string {
	var b strings.Builder
	avatarSetup(&b, siteURL, userID, appearanceURL, r15)
	b.WriteString(`return game:GetService("ThumbnailGenerator"):Click("OBJ", 1, 1, true, false)`)
	return b.String()
}

func placeScript(siteURL, placeURL string, w, h int, hideSky bool) string {
	return preamble(siteURL) +
		`local ok, err = pcall(function() game:Load(` + luaString(placeURL) + `) end)` + "\n" +
		`if not ok then error("place did not load: " .. tostring(err)) end` + "\n" +
		`if #game.Workspace:GetChildren() == 0 then error("place is empty, url probably failed") end` + "\n" +
		`return game:GetService("ThumbnailGenerator"):Click("PNG", ` + strconv.Itoa(w) + `, ` + strconv.Itoa(h) + `, ` + luaBool(hideSky) + `)`
}

// no game:GetObjects in this build, so CharacterFetch.ashx?assetId= + LoadCharacter(false) wears it
func itemScript(siteURL, appearanceURL string, w, h int) string {
	return itemSetup(siteURL, appearanceURL) +
		`return game:GetService("ThumbnailGenerator"):Click("PNG", ` + strconv.Itoa(w) + `, ` + strconv.Itoa(h) + `, true, false)`
}

func item3DScript(siteURL, appearanceURL string) string {
	return itemSetup(siteURL, appearanceURL) +
		`return game:GetService("ThumbnailGenerator"):Click("OBJ", 1, 1, true, false)`
}

func itemSetup(siteURL, appearanceURL string) string {
	return preamble(siteURL) +
		`local player = game:GetService("Players"):CreateLocalPlayer(0)` + "\n" +
		`player.CharacterAppearance = ` + luaString(appearanceURL) + "\n" +
		`player.CanLoadCharacterAppearance = true` + "\n" +
		loadCharacter
}
