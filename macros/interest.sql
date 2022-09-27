{% macro interest_amount(principal, loan_duration) %}
    ({{ principal }} * ({{ loan_duration }}/12) *  {{var('interest_rate') }} / 100)
{% endmacro %}