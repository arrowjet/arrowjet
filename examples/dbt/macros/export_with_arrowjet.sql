{% macro export_with_arrowjet(model_name) %}
  {#
    Post-run hook: export a dbt model to S3 via arrowjet after dbt run completes.

    Usage in dbt_project.yml:
      on-run-end:
        - "{{ export_with_arrowjet('sales_summary') }}"

    This macro runs a shell command via dbt's run_query. Since dbt hooks
    only execute SQL, we use a Redshift external function or a simple
    log statement here. The actual arrowjet export runs in the shell
    wrapper script (run_with_arrowjet.sh).

    For the shell-based pattern, this macro just logs what would be exported.
    The real export happens in run_with_arrowjet.sh after dbt run completes.
  #}
  {% do log("arrowjet export: " ~ model_name ~ " → s3://$STAGING_BUCKET/dbt-exports/" ~ model_name ~ "/", info=True) %}
  {% do run_query("select 1") %}
{% endmacro %}
