package river

import (
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type SourceConfig struct {
	Schema     string   `toml:"schema"`
	DestSchema string   `toml:"dest_schema"`
	Tables     []string `toml:"tables"`
}

type Config struct {
	SrcMyAddr     string `toml:"src_my_addr"`
	SrcMyUser     string `toml:"src_my_user"`
	SrcMyPassword string `toml:"src_my_pass"`
	SrcMyCharset  string `toml:"src_my_charset"`

	DestMyAddr     string `toml:"dest_my_addr"`
	DestMyUser     string `toml:"dest_my_user"`
	DestMyPassword string `toml:"dest_my_pass"`
	DestMyCharset  string `toml:"dest_my_charset"`

	StatAddr string `toml:"stat_addr"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	DumpExec       string `toml:"mysqldump"`
	SkipMasterData bool   `toml:"skip_master_data"`

	Sources []SourceConfig `toml:"source"`

	Rules []*Rule `toml:"rule"`

	BulkSize int `toml:"bulk_size"`

	FlushBulkTime TomlDuration `toml:"flush_bulk_time"`

	SkipNoPkTable bool `toml:"skip_no_pk_table"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

type TomlDuration struct {
	time.Duration
}

func (d *TomlDuration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
