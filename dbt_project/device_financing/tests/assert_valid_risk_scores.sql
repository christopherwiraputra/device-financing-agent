-- fails if any composite risk score is outside expected range
select application_id, composite_risk_score
from {{ ref('int_device_financing_enriched') }}
where composite_risk_score < 0
   or composite_risk_score > 3