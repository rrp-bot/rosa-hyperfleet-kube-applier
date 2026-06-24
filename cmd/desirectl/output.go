package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	sigsyaml "sigs.k8s.io/yaml"
)

type OutputFormat string

const (
	OutputTable OutputFormat = "table"
	OutputJSON  OutputFormat = "json"
	OutputYAML  OutputFormat = "yaml"
	OutputWide  OutputFormat = "wide"
)

func parseOutputFormat(s string) OutputFormat {
	switch s {
	case "json":
		return OutputJSON
	case "yaml":
		return OutputYAML
	case "wide":
		return OutputWide
	default:
		return OutputTable
	}
}

func printResourceJSON(w io.Writer, kubeContent []byte) error {
	var obj any
	if err := json.Unmarshal(kubeContent, &obj); err != nil {
		_, err := fmt.Fprintln(w, string(kubeContent))
		return err
	}
	formatted, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		_, err := fmt.Fprintln(w, string(kubeContent))
		return err
	}
	_, err = fmt.Fprintln(w, string(formatted))
	return err
}

func printResourceYAML(w io.Writer, kubeContent []byte) error {
	yamlBytes, err := sigsyaml.JSONToYAML(kubeContent)
	if err != nil {
		_, err := fmt.Fprintln(w, string(kubeContent))
		return err
	}
	_, err = fmt.Fprint(w, string(yamlBytes))
	return err
}

func printResourceListJSON(w io.Writer, items [][]byte) error {
	var objects []any
	for _, item := range items {
		var obj any
		if err := json.Unmarshal(item, &obj); err != nil {
			objects = append(objects, string(item))
		} else {
			objects = append(objects, obj)
		}
	}
	formatted, err := json.MarshalIndent(objects, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(formatted))
	return err
}

func printResourceListYAML(w io.Writer, items [][]byte) error {
	for i, item := range items {
		if i > 0 {
			fmt.Fprintln(w, "---")
		}
		yamlBytes, err := sigsyaml.JSONToYAML(item)
		if err != nil {
			fmt.Fprintln(w, string(item))
			continue
		}
		fmt.Fprint(w, string(yamlBytes))
	}
	return nil
}

type tableRow struct {
	name      string
	namespace string
	extra     map[string]string
}

func printResourceTable(resType string, rows []tableRow) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)

	columns := genericColumns()
	if cols, ok := typeColumns[resType]; ok {
		columns = cols
	}

	for _, col := range columns {
		fmt.Fprintf(w, "%s\t", col)
	}
	fmt.Fprintln(w)

	for _, row := range rows {
		for _, col := range columns {
			switch col {
			case "NAME":
				fmt.Fprintf(w, "%s\t", row.name)
			case "NAMESPACE":
				fmt.Fprintf(w, "%s\t", row.namespace)
			default:
				val := row.extra[col]
				if val == "" {
					val = "<none>"
				}
				fmt.Fprintf(w, "%s\t", val)
			}
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}

func genericColumns() []string {
	return []string{"NAME", "NAMESPACE"}
}

var typeColumns = map[string][]string{
	"deployments":  {"NAME", "NAMESPACE", "READY", "UP-TO-DATE", "AVAILABLE"},
	"pods":         {"NAME", "NAMESPACE", "STATUS", "RESTARTS"},
	"services":     {"NAME", "NAMESPACE", "TYPE", "CLUSTER-IP"},
	"configmaps":   {"NAME", "NAMESPACE", "DATA"},
	"secrets":      {"NAME", "NAMESPACE", "TYPE", "DATA"},
	"statefulsets": {"NAME", "NAMESPACE", "READY"},
	"daemonsets":   {"NAME", "NAMESPACE", "DESIRED", "CURRENT", "READY"},
}

func extractTableRow(resType string, kubeContent []byte) tableRow {
	var obj map[string]any
	row := tableRow{extra: make(map[string]string)}

	if err := json.Unmarshal(kubeContent, &obj); err != nil {
		return row
	}

	if metadata, ok := obj["metadata"].(map[string]any); ok {
		row.name, _ = metadata["name"].(string)
		row.namespace, _ = metadata["namespace"].(string)
	}

	switch resType {
	case "deployments":
		extractDeploymentFields(obj, &row)
	case "configmaps":
		extractConfigMapFields(obj, &row)
	case "secrets":
		extractSecretFields(obj, &row)
	case "services":
		extractServiceFields(obj, &row)
	case "pods":
		extractPodFields(obj, &row)
	case "statefulsets":
		extractStatefulSetFields(obj, &row)
	case "daemonsets":
		extractDaemonSetFields(obj, &row)
	}

	return row
}

func extractDeploymentFields(obj map[string]any, row *tableRow) {
	status, _ := obj["status"].(map[string]any)
	spec, _ := obj["spec"].(map[string]any)

	replicas := jsonInt(spec, "replicas")
	ready := jsonInt(status, "readyReplicas")
	updated := jsonInt(status, "updatedReplicas")
	available := jsonInt(status, "availableReplicas")

	row.extra["READY"] = fmt.Sprintf("%d/%d", ready, replicas)
	row.extra["UP-TO-DATE"] = fmt.Sprintf("%d", updated)
	row.extra["AVAILABLE"] = fmt.Sprintf("%d", available)
}

func extractConfigMapFields(obj map[string]any, row *tableRow) {
	data, _ := obj["data"].(map[string]any)
	row.extra["DATA"] = fmt.Sprintf("%d", len(data))
}

func extractSecretFields(obj map[string]any, row *tableRow) {
	data, _ := obj["data"].(map[string]any)
	typ, _ := obj["type"].(string)
	if typ == "" {
		typ = "Opaque"
	}
	row.extra["TYPE"] = typ
	row.extra["DATA"] = fmt.Sprintf("%d", len(data))
}

func extractServiceFields(obj map[string]any, row *tableRow) {
	spec, _ := obj["spec"].(map[string]any)
	row.extra["TYPE"], _ = spec["type"].(string)
	row.extra["CLUSTER-IP"], _ = spec["clusterIP"].(string)
}

func extractPodFields(obj map[string]any, row *tableRow) {
	status, _ := obj["status"].(map[string]any)
	row.extra["STATUS"], _ = status["phase"].(string)
	row.extra["RESTARTS"] = "0"
	if containerStatuses, ok := status["containerStatuses"].([]any); ok {
		total := 0
		for _, cs := range containerStatuses {
			if csMap, ok := cs.(map[string]any); ok {
				total += jsonInt(csMap, "restartCount")
			}
		}
		row.extra["RESTARTS"] = fmt.Sprintf("%d", total)
	}
}

func extractStatefulSetFields(obj map[string]any, row *tableRow) {
	status, _ := obj["status"].(map[string]any)
	spec, _ := obj["spec"].(map[string]any)
	replicas := jsonInt(spec, "replicas")
	ready := jsonInt(status, "readyReplicas")
	row.extra["READY"] = fmt.Sprintf("%d/%d", ready, replicas)
}

func extractDaemonSetFields(obj map[string]any, row *tableRow) {
	status, _ := obj["status"].(map[string]any)
	row.extra["DESIRED"] = fmt.Sprintf("%d", jsonInt(status, "desiredNumberScheduled"))
	row.extra["CURRENT"] = fmt.Sprintf("%d", jsonInt(status, "currentNumberScheduled"))
	row.extra["READY"] = fmt.Sprintf("%d", jsonInt(status, "numberReady"))
}

func jsonInt(m map[string]any, key string) int {
	if m == nil {
		return 0
	}
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}
