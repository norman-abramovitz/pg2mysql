package pg2mysql

type Config struct {
	Dest struct {
		Flavor    string `yaml:"flavor"`
		Database  string `yaml:"database"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Host      string `yaml:"host"`
		Port      int    `yaml:"port"`
		RoundTime bool   `yaml:"round_time"`
		SSLMode   string `yaml:"ssl_mode"`
	} `yaml:"dest"`

	Source struct {
		Flavor   string `yaml:"flavor"`
		Database string `yaml:"database"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		SSLMode  string `yaml:"ssl_mode"`
	} `yaml:"source"`
}
