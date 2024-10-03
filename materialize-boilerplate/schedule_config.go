package boilerplate

import (
	"fmt"
	"time"

	"github.com/estuary/connectors/go/schedule"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSyncFrequency = "30m"
	slowInterval         = "4h" // interval to use when the "Fast Sync" is not enabled
)

// ScheduleConfig is the configuration for a materialization acknowledgement
// schedule. Providing only a SyncFrequency will result in a fixed schedule;
// otherwise an alternating schedule will be used.
type ScheduleConfig struct {
	SyncFrequency       string `json:"syncFrequency,omitempty" jsonschema:"title=Sync Frequency,enum=0s,enum=30s,enum=5m,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h" jsonschema_extras:"order=0"`
	Timezone            string `json:"timezone,omitempty" jsonschema:"title=Timezone" jsonschema_extras:"order=1"`
	FastSyncStartTime   string `json:"fastSyncStartTime,omitempty" jsonschema:"title=Fast Sync Start Time" jsonschema_extras:"pattern=^(0?[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$,order=2"`
	FastSyncStopTime    string `json:"fastSyncStopTime,omitempty" jsonschema:"title=Fast Sync Stop Time" jsonschema_extras:"pattern=^(0?[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$,order=3"`
	FastSyncEnabledDays string `json:"fastSyncEnabledDays,omitempty" jsonschema:"title=Fast Sync Enabled Days" jsonschema_extras:"order=4"`
}

func (ScheduleConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "SyncFrequency":
		return "Frequency at which transactions are executed when the materialization is fully caught up and streaming changes. May be enabled only for certain time periods and days of the week if configured below; otherwise it is effective 24/7. Defaults to 30 minutes if unset."
	case "Timezone":
		return "Timezone applicable to sync time windows and active days. Must be a valid IANA time zone name or +HH:MM offset."
	case "FastSyncStartTime":
		return `Time of day that transactions begin executing at the configured Sync Frequency. Prior to this time transactions will be executed more slowly. Must be in the form of '09:00'.`
	case "FastSyncStopTime":
		return "Time of day that transactions stop executing at the configured Sync Frequency. After this time transactions will be executed more slowly. Must be in the form of '17:00'."
	case "FastSyncEnabledDays":
		return "Days of the week that the configured Sync Frequency is active. On days that are not enabled, transactions will be executed more slowly for the entire day. Examples: 'M-F' (Monday through Friday, inclusive), 'M,W,F' (Monday, Wednesday, and Friday), 'Su-T,Th-S' (Sunday through Tuesday, inclusive; Thursday through Saturday, inclusive). All days are enabled if unset."
	default:
		return ""
	}
}

func (c ScheduleConfig) Validate() error {
	if _, err := createSchedule(c, nil); err != nil {
		return fmt.Errorf("validating schedule configuration: %w", err)
	}
	return nil
}

func createSchedule(cfg ScheduleConfig, jitter []byte) (schedule.Schedule, error) {
	alternatingSchedule := false
	if cfg.FastSyncStartTime != "" || cfg.FastSyncStopTime != "" || cfg.FastSyncEnabledDays != "" || cfg.Timezone != "" {
		if cfg.SyncFrequency == "" || cfg.Timezone == "" || cfg.FastSyncStartTime == "" || cfg.FastSyncStopTime == "" {
			return nil, fmt.Errorf("must provide 'timezone', 'syncFrequency', 'fastSyncStartTime', and 'fastSyncStopTime' when configuring sync frequency active times")
		}
		alternatingSchedule = true
	}

	freq := cfg.SyncFrequency
	if freq == "" {
		freq = defaultSyncFrequency
	}

	if !alternatingSchedule {
		parsedInterval, err := time.ParseDuration(freq)
		if err != nil {
			return nil, fmt.Errorf("parsing interval in CreateSchedule: %w", err)
		}

		if parsedInterval == 0 {
			// Special case: There is a 0 frequency and no alternating schedule,
			// so there is no delay at all.
			return nil, nil
		}

		// There is a frequency set but they aren't using an alternating
		// schedule, so just create a simple fixed schedule.
		sched, err := schedule.NewFixedSchedule(freq, jitter)
		if err != nil {
			return nil, err
		}

		log.WithFields(log.Fields{
			"syncFrequency": freq,
		}).Info("created fixed schedule for acknowledgements")

		return sched, nil
	}

	sched, err := schedule.NewAlternatingSchedule(
		slowInterval,
		cfg.SyncFrequency,
		cfg.FastSyncStartTime,
		cfg.FastSyncStopTime,
		cfg.FastSyncEnabledDays,
		cfg.Timezone,
		jitter,
	)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"config": cfg,
	}).Info("created alternating schedule for acknowledgements")

	return sched, nil
}
