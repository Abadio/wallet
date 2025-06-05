#!/bin/bash

# Script para executar a suíte de testes e filtrar logs relevantes
TEST_CLASS="RedisDltConsumerIntegrationTest"
PROFILE="integration"
LOG_FILE="debug_suite.log"
FILTERED_LOG="filtered_suite.log"
SUREFIRE_REPORT="target/surefire-reports/TEST-com.recargapay.wallet.query.service.RedisDltConsumerIntegrationTest.xml"

# Limpar logs e relatórios antigos
rm -f "$LOG_FILE" "$FILTERED_LOG"

# Verificar memória antes
echo "Memória antes:" > "$FILTERED_LOG"
free -m >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

# Executar a suíte com debug
mvn clean test -Dtest="$TEST_CLASS" -Dspring.profiles.active="$PROFILE" -X > "$LOG_FILE" 2>&1

# Filtrar logs relevantes
echo "=== Resultado da Suíte ===" >> "$FILTERED_LOG"
grep -E "Tests run:.*Failures:.*Errors:.*Skipped" "$LOG_FILE" >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

echo "=== Testes que Falharam ===" >> "$FILTERED_LOG"
grep -E "Tests run:.*Failures:.*Errors:.*Skipped.*in com.recargapay.wallet.query.service.RedisDltConsumerIntegrationTest" -A 10 "$LOG_FILE" | grep -E "test.*\(" >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

echo "=== Erros do RedisDltConsumer ===" >> "$FILTERED_LOG"
grep -E "com.recargapay.wallet.query.dlt.RedisDltConsumer.*(ERROR|WARN)" "$LOG_FILE" >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

echo "=== Logs de Consumo do Kafka ===" >> "$FILTERED_LOG"
grep -E "Received DLT message|Successfully re-sent event|Failed to re-send event" "$LOG_FILE" >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

echo "=== Métricas do MeterRegistry ===" >> "$FILTERED_LOG"
grep -E "Metric:.*dlt\.redis\." "$LOG_FILE" >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

echo "=== Logs de Limpeza do Kafka ===" >> "$FILTERED_LOG"
grep -E "Clearing messages from topics|Consumed and cleared.*records" "$LOG_FILE" >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

echo "=== Erros Gerais ===" >> "$FILTERED_LOG"
grep -E "ERROR.*(Exception|Failed)" "$LOG_FILE" | grep -v "com.recargapay.wallet.query.dlt.RedisDltConsumer" >> "$FILTERED_LOG"
echo "" >> "$FILTERED_LOG"

# Extrair falhas do relatório Surefire
if [ -f "$SUREFIRE_REPORT" ]; then
    echo "=== Falhas no Relatório Surefire ===" >> "$FILTERED_LOG"
    grep -E "<failure|<error" "$SUREFIRE_REPORT" -A 5 >> "$FILTERED_LOG"
    echo "" >> "$FILTERED_LOG"
fi

# Verificar memória após
echo "Memória após:" >> "$FILTERED_LOG"
free -m >> "$FILTERED_LOG"

# Resumir tamanho do log filtrado
echo "Tamanho do log filtrado: $(wc -l < "$FILTERED_LOG") linhas" >> "$FILTERED_LOG"