
import json

def getFieldsToBeDeleted(fields, type):
    lookup = {}
    for field in fields:
        lookup[field[0] + field[1] + type] = field
    new_fields = []
    for k, v in lookup.items():
        if "." in k:
            # if it has corresponding underscore attribute then capture it
            if k.replace(".", "_") in lookup.keys():
                x = list(v)
                x.append(type)
                new_fields.append(x)
            
            ## log if one is materialized but other is not
    return new_fields

def getFields(client):
    print("connected to clickhouse \n")

    QUERY = "SELECT DISTINCT name, datatype from signoz_logs.distributed_logs_attribute_keys group by name, datatype"
    attributes = client.execute(QUERY)


    QUERY = "SELECT DISTINCT name, datatype from signoz_logs.distributed_logs_resource_keys group by name, datatype"
    resources = client.execute(QUERY)

    attributes = getFieldsToBeDeleted(attributes, "attributes")
    resources = getFieldsToBeDeleted(resources, "resources")

    fields = {}
    for field in attributes + resources:  
        fields[field[2]+"_"+field[1].lower()+"_" + field[0].replace('.', '_')] = field
    return fields

def get_type(type):
    if type == "tag":
        return "attributes"
    return "resources"

def update_field(field, lookup):
    updated = False
    if field["type"] == None or field["dataType"] == None or field["key"] == None:
        return field, updated
    name = get_type(field["type"])+ "_"+ field["dataType"].lower()+"_" + field["key"]
    updated = False
    if name in lookup.keys():
        field["key"] = lookup[name][0]
        updated = True
    return field, updated