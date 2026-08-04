{{- define "keycloak.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "keycloak.fullname" -}}
{{- default (include "keycloak.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "keycloak.labels" -}}
app.kubernetes.io/name: {{ include "keycloak.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: identity
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "keycloak.selectorLabels" -}}
app.kubernetes.io/name: {{ include "keycloak.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "keycloak.realmJson" -}}
{
  "realm": {{ .Values.realm.name | quote }},
  "displayName": {{ .Values.realm.displayName | quote }},
  "enabled": true,
  "registrationAllowed": false,
  "clients": [
    {
      "clientId": {{ .Values.realm.clientId | quote }},
      "name": "Ecommerce Frontend",
      "enabled": true,
      "protocol": "openid-connect",
      "publicClient": true,
      "standardFlowEnabled": true,
      "directAccessGrantsEnabled": true,
      "rootUrl": {{ .Values.realm.rootUrl | quote }},
      "redirectUris": [
        {{- range $index, $uri := .Values.realm.validRedirectUris }}
        {{- if $index }},{{ end }}{{ $uri | quote }}
        {{- end }}
      ],
      "webOrigins": [
        {{- range $index, $origin := .Values.realm.webOrigins }}
        {{- if $index }},{{ end }}{{ $origin | quote }}
        {{- end }}
      ]
    }
  ],
  "users": [
    {
      "username": {{ .Values.realm.testUser.username | quote }},
      "enabled": true,
      "email": {{ .Values.realm.testUser.email | quote }},
      "emailVerified": true,
      "credentials": [
        {
          "type": "password",
          "value": {{ .Values.realm.testUser.password | quote }},
          "temporary": false
        }
      ]
    }
  ]
}
{{- end -}}
