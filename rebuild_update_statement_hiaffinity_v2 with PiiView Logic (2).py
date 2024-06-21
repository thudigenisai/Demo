def rebuild_update_statement(update_statement_list_generator, root_records_pd, object_attributes, cdc_control_column_names):
    # Convert the changedatetime column from string to datetime format
    root_records_pd['changedatetime'] = pd.to_datetime(root_records_pd['changedatetime'], format="%Y-%m-%d %H:%M:%S.%f")

    result_spark_format = []  # Final result as list
    result_pd_format = root_records_pd.head(1).copy()[0:0]  # A staging Pandas DataFrame for better performance

    update_statement_list = list(update_statement_list_generator)  # Get all update statements as list
    update_statement_list.sort(key=lambda row: pd.to_datetime(row["changedatetime"]), reverse=False)  # Sort by changedatetime

    for update_statement in update_statement_list:  # Iterate each raw update statement 
        recid = update_statement["_recid"]

        # Find historical root row
        historical_root_row = result_pd_format[result_pd_format["_recid"] == recid].sort_values(by='changedatetime', ascending=False).head(1)
        if historical_root_row.empty:
            historical_root_row = root_records_pd[root_records_pd["_recid"] == recid].sort_values(by='changedatetime', ascending=False).head(1)

        if historical_root_row.empty:
            result_spark_format.append(update_statement)
            continue

        # Start rebuilding the fields
        changed_fields = update_statement["changed_fields"].split(";")  # Get all changed fields
        new_row_pd = {}
        new_row_spark = {}

        for field_name in update_statement.__fields__:  # Rebuild every field for the raw update statement
            org_row_value = update_statement[field_name]  # Get raw field value

            if field_name in changed_fields or field_name.lower() in cdc_control_column_names or org_row_value is not None:
                field_value = org_row_value
            else:
                field_value = historical_root_row.iloc[0][field_name]

                if object_attributes[field_name.lower()] == "date" and field_value is not None and re.match(r'^\d{4}-\d{2}-\d{2}$', field_value):
                    field_value = str(field_value) + " 00:00:00.0000000"

            new_row_pd[field_name] = pd.to_datetime(field_value, format="%Y-%m-%d %H:%M:%S.%f") if field_name == "changedatetime" and isinstance(field_value, str) else field_value
            new_row_spark[field_name] = field_value.strftime("%Y-%m-%d %H:%M:%S.%f") if field_name == "changedatetime" and isinstance(field_value, pd._libs.tslibs.timestamps.Timestamp) else field_value

        result_pd_format = pd.concat([result_pd_format, pd.DataFrame([new_row_pd])], ignore_index=True)  # Insert into staging DataFrame
        result_spark_format.append(Row(**new_row_spark))  # Insert into final result list for Spark job

    return iter(result_spark_format)
