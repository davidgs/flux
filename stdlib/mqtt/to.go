package mqtt

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/internal/pkg/syncutil"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
	protocol "github.com/influxdata/line-protocol"
	"github.com/pkg/errors"
)

const (
	ToMQTTKind           = "toMQTT"
	DefaultToMQTTTimeout = 1 * time.Second
)

func init() {
	toMQTTSignature := flux.FunctionSignature(
		map[string]semantic.PolyType{
			"broker":       semantic.String,
			"topic":        semantic.String,
			"message":      semantic.String,
			"format":       semantic.String,
			"qos":          semantic.Int,
			"clientid":     semantic.String,
			"username":     semantic.String,
			"password":     semantic.String,
			"name":         semantic.String,
			"timeout":      semantic.Duration,
			"timeColumn":   semantic.String,
			"tagColumns":   semantic.NewArrayPolyType(semantic.String),
			"valueColumns": semantic.NewArrayPolyType(semantic.String),
		},
		[]string{"broker"},
	)

	flux.RegisterPackageValue("mqtt", "to", flux.FunctionValueWithSideEffect(ToMQTTKind, createToMQTTOpSpec, toMQTTSignature))
	flux.RegisterOpSpec(ToMQTTKind, func() flux.OperationSpec { return &ToMQTTOpSpec{} })
	plan.RegisterProcedureSpecWithSideEffect(ToMQTTKind, newToMQTTProcedure, ToMQTTKind)
	execute.RegisterTransformation(ToMQTTKind, createToMQTTTransformation)
}

// DefaultToMQTTUserAgent is the default user agent used by ToMqtt
var DefaultToMQTTUserAgent = "fluxd/dev"

//

//func newToMQTTClient() *MQTT.Client {

//}

// var toMQTTKeepAliveClient = newToMQTTClient()

// this is used so we can get better validation on marshaling, innerToMQTTOpSpec and ToMQTTOpSpec
// need to have identical fields
type innerToMQTTOpSpec ToMQTTOpSpec

type ToMQTTOpSpec struct {
	Broker       string        `json:"broker"`
	Name         string        `json:"name"`
	Topic        string        `json:"topic"`
	Message      string        `json:"message"`
	Format       string        `json:"format"`
	ClientID     string        `json:"clientid"`
	Username     string        `json:"username"`
	Password     string        `json:"password"`
	QoS          int           `json:"qos"`
	NameColumn   string        `json:"nameColumn"` // either name or name_column must be set, if none is set try to use the "_measurement" column.
	Timeout      time.Duration `json:"timeout"`    // default to something reasonable if zero
	NoKeepAlive  bool          `json:"noKeepAlive"`
	TimeColumn   string        `json:"timeColumn"`
	TagColumns   []string      `json:"tagColumns"`
	ValueColumns []string      `json:"valueColumns"`
}

// ReadArgs loads a flux.Arguments into ToMQTTOpSpec.  It sets several default values.
// If the time_column isn't set, it defaults to execute.TimeColLabel.
// If the value_column isn't set it defaults to a []string{execute.DefaultValueColLabel}.
func (o *ToMQTTOpSpec) ReadArgs(args flux.Arguments) error {
	var err error
	o.Broker, err = args.GetRequiredString("broker")
	if err != nil {
		return err
	}
	var ok bool
	o.Topic, ok, err = args.GetString("topic")
	if err != nil {
		return err
	}
	o.Message, ok, err = args.GetString("message")
	if err != nil {
		return err
	}
	if len(o.Message) > 0 && len(o.Topic) <= 0 { // if you are sending a static mesage, must define a topic.
		return fmt.Errorf("Topic required with message %s", o.Message)
	}
	o.Format, ok, err = args.GetString("format")
	if err != nil {
		return err
	}
	if len(o.Format) > 0 {
		if o.Format != "lineProtocol" && o.Format != "JSON" {
			return fmt.Errorf("Format must be lineProtocol or JSON, not %s", o.Format)
		}
	}
	o.Name, ok, err = args.GetString("name")
	if err != nil {
		return err
	}
	if !ok {
		o.NameColumn, ok, err = args.GetString("nameColumn")
		if err != nil {
			return err
		}
		if !ok {
			o.NameColumn = "_measurement"
		}
	}

	o.ClientID, ok, err = args.GetString("clientid")
	if err != nil {
		return err
	}
	if !ok {
		o.ClientID = "flux-mqtt"
	}

	o.Username, ok, err = args.GetString("username")
	if err != nil {
		return err
	}

	o.Password, ok, err = args.GetString("password")
	if err != nil {
		return err
	}
	if len(o.Username) > 0 && len(o.Password) <= 0 {
		return fmt.Errorf("Password required with username %s", o.Username)
	}
	q, ok, err := args.GetInt("qos")
	if err != nil {
		return err
	}
	if !ok {
		o.QoS = 0
	} else {
		o.QoS = int(q)
	}
	if o.QoS < 0 || o.QoS > 3 {
		o.QoS = 0 // default to 0 if some random value is passed
	}
	timeout, ok, err := args.GetDuration("timeout")
	if err != nil {
		return err
	}
	if !ok {
		o.Timeout = DefaultToMQTTTimeout
	} else {
		o.Timeout = time.Duration(timeout)
	}

	o.TimeColumn, ok, err = args.GetString("timeColumn")
	if err != nil {
		return err
	}
	if !ok {
		o.TimeColumn = execute.DefaultTimeColLabel
	}

	tagColumns, ok, err := args.GetArray("tagColumns", semantic.String)
	if err != nil {
		return err
	}
	o.TagColumns = o.TagColumns[:0]
	if ok {
		for i := 0; i < tagColumns.Len(); i++ {
			o.TagColumns = append(o.TagColumns, tagColumns.Get(i).Str())
		}
		sort.Strings(o.TagColumns)
	}

	valueColumns, ok, err := args.GetArray("valueColumns", semantic.String)
	if err != nil {
		return err
	}
	o.ValueColumns = o.ValueColumns[:0]

	if !ok || valueColumns.Len() == 0 {
		o.ValueColumns = append(o.ValueColumns, execute.DefaultValueColLabel)
	} else {
		for i := 0; i < valueColumns.Len(); i++ {
			o.ValueColumns = append(o.ValueColumns, valueColumns.Get(i).Str())
		}
		sort.Strings(o.ValueColumns)
	}
	fmt.Println("MQTT Options set")
	return err
}

func createToMQTTOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	s := new(ToMQTTOpSpec)
	if err := s.ReadArgs(args); err != nil {
		return nil, err
	}
	fmt.Println("MQTT Created")
	return s, nil
}

// UnmarshalJSON unmarshals and validates toMQTTOpSpec into JSON.
func (o *ToMQTTOpSpec) UnmarshalJSON(b []byte) (err error) {
	if err = json.Unmarshal(b, (*innerToMQTTOpSpec)(o)); err != nil {
		return err
	}
	u, err := url.ParseRequestURI(o.Broker)
	if err != nil {
		return err
	}
	if !(u.Scheme == "tcp" || u.Scheme == "ws" || u.Scheme == "tls") {
		return fmt.Errorf("scheme must be tcp or ws or tls but was %s", u.Scheme)
	}
	fmt.Println("MQTT Unmarshall")
	return nil
}

func (ToMQTTOpSpec) Kind() flux.OperationKind {
	return ToMQTTKind
}

type ToMQTTProcedureSpec struct {
	plan.DefaultCost
	Spec *ToMQTTOpSpec
}

func (o *ToMQTTProcedureSpec) Kind() plan.ProcedureKind {
	return ToMQTTKind
}

func (o *ToMQTTProcedureSpec) Copy() plan.ProcedureSpec {
	s := o.Spec
	res := &ToMQTTProcedureSpec{
		Spec: &ToMQTTOpSpec{
			Broker:       s.Broker,
			Topic:        s.Topic,
			Name:         s.Name,
			QoS:          s.QoS,
			Username:     s.Username,
			Password:     s.Password,
			NameColumn:   s.NameColumn,
			Timeout:      s.Timeout,
			NoKeepAlive:  s.NoKeepAlive,
			TimeColumn:   s.TimeColumn,
			TagColumns:   append([]string(nil), s.TagColumns...),
			ValueColumns: append([]string(nil), s.ValueColumns...),
		},
	}
	fmt.Println("MQTT Copy")
	return res
}

func newToMQTTProcedure(qs flux.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ToMQTTOpSpec)
	if !ok && spec != nil {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	fmt.Println("MQTT Procedure")
	return &ToMQTTProcedureSpec{Spec: spec}, nil
}

func createToMQTTTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*ToMQTTProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewToMQTTTransformation(d, cache, s)
	fmt.Println("MQTT Transformation")
	return t, d, nil
}

type ToMQTTTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache
	spec  *ToMQTTProcedureSpec
}

func (t *ToMQTTTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

func NewToMQTTTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *ToMQTTProcedureSpec) *ToMQTTTransformation {
	return &ToMQTTTransformation{
		d:     d,
		cache: cache,
		spec:  spec,
	}
}

type toMqttMetric struct {
	tags   []*protocol.Tag
	fields []*protocol.Field
	name   string
	t      time.Time
}

func (m *toMqttMetric) TagList() []*protocol.Tag {
	return m.tags
}
func (m *toMqttMetric) FieldList() []*protocol.Field {
	return m.fields
}

func (m *toMqttMetric) truncateTagsAndFields() {
	m.fields = m.fields[:0]
	m.tags = m.tags[:0]
}

func (m *toMqttMetric) Name() string {
	return m.name
}

func (m *toMqttMetric) Time() time.Time {
	return m.t
}

type idxType struct {
	Idx  int
	Type flux.ColType
}

func (t *ToMQTTTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	// set up the MQTT options.
	fmt.Println("Start MQTT Process")
	opts := MQTT.NewClientOptions().AddBroker(t.spec.Spec.Broker)
	if len(t.spec.Spec.ClientID) > 0 {
		opts.SetClientID(t.spec.Spec.ClientID)
	} else {
		opts.SetClientID("flux-mqtt")
	}
	if t.spec.Spec.Timeout > 0 {
		opts.SetConnectTimeout(t.spec.Spec.Timeout)
	}
	if len(t.spec.Spec.Username) > 0 {
		opts.SetUsername(t.spec.Spec.Username)
	}
	if len(t.spec.Spec.Password) > 0 {
		opts.SetPassword(t.spec.Spec.Password)
	}
	mqttTopic := t.spec.Spec.Topic

	client := MQTT.NewClient(opts)
	if len(t.spec.Spec.Message) > 0 {
		//create and start a client using the above ClientOptions
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		token := client.Publish(t.spec.Spec.Topic, 0, false, t.spec.Spec.Message)
		token.Wait()
		client.Disconnect(250)
		return nil
	}
	pr, pw := io.Pipe() // TODO: replce the pipe with something faster
	m := &toMqttMetric{}
	e := protocol.NewEncoder(pw)
	e.FailOnFieldErr(true)
	e.SetFieldSortOrder(protocol.SortFields)
	cols := tbl.Cols()
	labels := make(map[string]idxType, len(cols))
	for i, col := range cols {
		labels[col.Label] = idxType{Idx: i, Type: col.Type}
	}

	// do time
	timeColLabel := t.spec.Spec.TimeColumn
	timeColIdx, ok := labels[timeColLabel]

	if !ok {
		return errors.New("Could not get time column")
	}
	if timeColIdx.Type != flux.TTime {
		return fmt.Errorf("column %s is not of type %s", timeColLabel, timeColIdx.Type)
	}
	var measurementNameCol string
	if t.spec.Spec.Name == "" {
		measurementNameCol = t.spec.Spec.NameColumn
	}

	// check if each col is a tag or value and cache this value for the loop
	colMetadatas := tbl.Cols()
	isTag := make([]bool, len(colMetadatas))
	isValue := make([]bool, len(colMetadatas))

	for i, col := range colMetadatas {
		valIdx := sort.SearchStrings(t.spec.Spec.ValueColumns, col.Label)
		isValue[i] = valIdx < len(t.spec.Spec.ValueColumns) && t.spec.Spec.ValueColumns[valIdx] == col.Label

		tagIdx := sort.SearchStrings(t.spec.Spec.TagColumns, col.Label)
		isTag[i] = tagIdx < len(t.spec.Spec.TagColumns) && t.spec.Spec.TagColumns[tagIdx] == col.Label
	}

	builder, new := t.cache.TableBuilder(tbl.Key())
	if new {
		if err := execute.AddTableCols(tbl, builder); err != nil {
			return err
		}
	}

	var wg syncutil.WaitGroup
	wg.Do(func() error {
		fmt.Println("Process.Do")
		m.name = t.spec.Spec.Name
		err := tbl.Do(func(er flux.ColReader) error {
			l := er.Len()
			for i := 0; i < l; i++ {
				m.truncateTagsAndFields()
				for j, col := range er.Cols() {
					switch {
					case col.Label == timeColLabel:
						m.t = values.Time(er.Times(j).Value(i)).Time()
					case measurementNameCol != "" && measurementNameCol == col.Label:
						if col.Type != flux.TString {
							return errors.New("invalid type for measurement column")
						}
						m.name = er.Strings(j).ValueString(i)
					case isTag[j]:
						if col.Type != flux.TString {
							return errors.New("invalid type for tag column")
						}
						m.tags = append(m.tags, &protocol.Tag{Key: col.Label, Value: er.Strings(j).ValueString(i)})

					case isValue[j]:
						switch col.Type {
						case flux.TFloat:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Floats(j).Value(i)})
						case flux.TInt:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Ints(j).Value(i)})
						case flux.TUInt:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.UInts(j).Value(i)})
						case flux.TString:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Strings(j).ValueString(i)})
						case flux.TTime:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: values.Time(er.Times(j).Value(i))})
						case flux.TBool:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Bools(j).Value(i)})
						default:
							return fmt.Errorf("invalid type for column %s", col.Label)
						}
					}
				}
				_, err := e.Encode(m)
				if err != nil {
					return err
				}

				if err := execute.AppendRecord(i, er, builder); err != nil {
					return err
				}
			}
			return nil
		})
		if e := pw.Close(); e != nil && err == nil {
			err = e
		}
		return err
	})

	//start a client using the above ClientOptions
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	p := make([]byte, 2024)
	var message strings.Builder
	lines := 0
	// messages come in as triples: measurement & tags, values, timestamp
	// put them all together and you have line-protocol!
	for {
		n, err := pr.Read(p)
		if err != nil {
			if err == io.EOF {
				message.WriteString(string(p[:n]))
				break
			}
			client.Disconnect(250)
			return err
		}
		message.WriteString(string(p[:n])) // handle leftovers
		lines += 1
		if lines > 2 { // post message after full row is read.
			if len(mqttTopic) <= 0 { // No topic set? Create topic out of tags
				mqttTopic = m.createTopic(message.String())
			}
			if t.spec.Spec.Format == "JSON" { // format message as a JSON
				message = m.formatJSON(message.String())
			}
			token := client.Publish(mqttTopic, 0, false, message.String())
			token.Wait()
			lines = 0
			message.Reset()
		}
	}
	if len(message.String()) > 0 { // if any leftover messages are there, write them out.
		if len(mqttTopic) <= 0 { // create topic out of tags
			mqttTopic = m.createTopic(message.String())
		}
		token := client.Publish(mqttTopic, 0, false, message.String())
		token.Wait()
	}
	if err := wg.Wait(); err != nil {
		client.Disconnect(250)
		return err
	}
	client.Disconnect(250)
	fmt.Println("Process.Done")
	return nil

}

// format the message as json
func (t *toMqttMetric) formatJSON(message string) strings.Builder {
	var b strings.Builder
	b.WriteString("{ \"measurement\": \"")
	at := strings.Split(message, " ")
	as := strings.Split(at[0], ",")
	if len(as) > 1 {
		b.WriteString(as[0])
		b.WriteString("\"")
		b.WriteString(t.parseTags(as[1]))
	} else {
		b.WriteString(at[0])
		b.WriteString("\"")
	}
	b.WriteString(", \"values\": { ")
	as = strings.Split(at[1], ",")
	l := len(as) - 1
	if l > 1 {
		for i := 1; i < l; i++ {
			toke := strings.Split(as[i], "=")
			if i > 1 {
				b.WriteString(", ")
			}
			b.WriteString(toke[0])
			b.WriteString(": ")
			if _, err := strconv.Atoi(toke[1]); err == nil {
				b.WriteString(toke[1])
			} else if _, err := strconv.ParseBool(toke[1]); err == nil {
				b.WriteString("\"")
				b.WriteString(toke[1])
				b.WriteString("\"")
			} else if _, err := strconv.ParseFloat(toke[1], 64); err == nil {
				b.WriteString(toke[1])
			} else if _, err := strconv.ParseInt(toke[1], 10, 64); err == nil {
				b.WriteString(toke[1])
			} else {
				b.WriteString("\"")
				b.WriteString(toke[1])
				b.WriteString("\"")
			}
		}
	} else {
		as = strings.Split(at[1], "=")
		b.WriteString("\"")
		b.WriteString(as[0])
		b.WriteString("\": ")
		if _, err := strconv.Atoi(as[1]); err == nil {
			b.WriteString(as[1])
		} else if _, err := strconv.ParseBool(as[1]); err == nil {
			b.WriteString("\"")
			b.WriteString(as[1])
			b.WriteString("\"")
		} else if _, err := strconv.ParseFloat(as[1], 64); err == nil {
			b.WriteString(as[1])
		} else if _, err := strconv.ParseInt(as[1], 10, 64); err == nil {
			b.WriteString(as[1])
		} else {
			b.WriteString("\"")
			b.WriteString(as[1])
			b.WriteString("\"")
		}
	}
	b.WriteString("}, \"time\": ")
	b.WriteString(at[2])
	b.WriteString("}")
	return b

}

// add all tags to the JSON
func (t *toMqttMetric) parseTags(tags string) string {
	var mess strings.Builder
	mess.WriteString("\", \"tags\": { ")
	as := strings.Split(tags, ",")
	l := len(as) - 1
	if l > 1 {
		for i := 1; i < l; i++ {
			toke := strings.Split(tags, "=")
			if i > 1 {
				mess.WriteString(", ")
			}
			mess.WriteString("\"")
			mess.WriteString(toke[0])
			mess.WriteString("\": \"")
			mess.WriteString(toke[1])
			mess.WriteString("\"")
		}
	} else {
		as = strings.Split(tags, "=")
		mess.WriteString("\"")
		mess.WriteString(as[0])
		mess.WriteString("\": \"")
		mess.WriteString(as[1])
		mess.WriteString("\"")
	}
	mess.WriteString("}")
	return mess.String()
}

// creates a topic consisting of measurement/tagname/tagvalue for all tags
func (t *toMqttMetric) createTopic(topicString string) string {
	var top strings.Builder
	tt := strings.Split(topicString, " ")
	tt = strings.Split(tt[0], ",")
	top.WriteString("/")
	top.WriteString(tt[0])
	l := len(tt) - 1
	for i := 1; i < l; i++ {
		toke := strings.Split(tt[i], "=")
		top.WriteString("/")
		top.WriteString(toke[0])
		top.WriteString("/")
		top.WriteString(toke[1])
	}
	return top.String()
}
func (t *ToMQTTTransformation) UpdateWatermark(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateWatermark(pt)
}
func (t *ToMQTTTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *ToMQTTTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
