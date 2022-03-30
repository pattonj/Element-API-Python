import json
import requests


def api_user_search(
    client: str,
    api: str,
    feature: str,
    segment: str | dict,
    project: list[str] = ["_id"],
) -> dict:
    """
    Provides a function to easily export users from an Element451 instance.
    **Uses the json and requests library.**

    Parameters:
        client (str): The name of the client instance
        api (str): The element API. Should be 'api.451.io'
        feature (str): The feature ID to access data
        segment (str or dict): The segment ID to export or inline segment. Example: 'client.segments.40291'
        ---- optional ----
        Project (list): The fields you want to list for each user. You can only project: [_id, first_name, last_name, email]. Defaults to [_id].
    Returns:
        The json data from the request. If more than 5,000 contacts were requested, the data is combined."""

    if client == "" or api == "" or feature == "" or segment == "":
        raise Exception("A required parameter was blank.")

    my_content_type = "application/json"
    my_api_header = {"Content-Type": my_content_type, "Feature": feature}
    my_api_url = f"https://{client}.{api}/v2/users/export/search"

    all_user_payload_results = ""
    next_last_id = ""
    results_data = ""
    run = True
    while run:
        my_payload = _create_api_search_payload(segment, project, next_last_id)
        result = requests.post(my_api_url, headers=my_api_header, data=my_payload)
        # If it's not 200, we weren't successful.
        if result.status_code != 200:
            raise Exception(
                f"User search request failed for: {my_api_url} {result.content}"
            )
        # If next_last_id is part of it, there will be more to get.
        if "next_last_id" in result.json()["meta"]:
            next_last_id = result.json()["meta"]["next_last_id"]
            if results_data == "":
                results_data = result.json()
            # If results_data is not empty, we need to append.
            else:
                for x in result.json()["data"]:
                    results_data["data"].append(x)
        # If next_last_id is not part of it, we've hit the end of the list.
        else:
            all_user_payload_results = result.json()
            # Checking if we retrieved data before this call.
            if results_data != "":
                for x in results_data["data"]:
                    all_user_payload_results["data"].append(x)
            all_user_payload_results["meta"]["filtered_total"] = (
                all_user_payload_results["meta"]["filtered_total"]
                + all_user_payload_results["meta"]["filtered_total"]
            )

            run = False

    return all_user_payload_results


def _create_api_search_payload(
    segment_id: str, project: list[str], last_id: str = ""
) -> dict:
    """Used to set the data for the search API request"""
    my_payload = json.dumps(
        {"item": {"segment": segment_id, "project": project, "last_id": last_id}}
    )
    return my_payload


def api_data_request(
    client: str,
    api: str,
    feature: str,
    segment: str | list[str],
    template: str | dict,
    column_key: str = "",
    unwind: dict = {},
) -> dict:
    """
    Provides a function to easily export data from an Element451 instance.
    **Uses the json and requests library.**

    Parameters:
        client (str): The name of the client instance
        api (str): The element API. Should be 'api.451.io'
        feature (str): The feature ID to access data
        segment (str or list): The segment ID to export or list of users. Example: 'client.segments.40291' or ['620e9b3534e7f364a5726292']
        template (str or dict/list): The template ID or layout to export. Example: 'client.template.8071' or '{ "columns": [ { "field": "Email", "mode": "slug", "slug": "user-email-address" } ] }'
        ---- optional ----
        column_key (string): Can be set to either 'slug' or 'field. If empty, field is used.
        unwind (dict): The entity to unwind. Example: {"root" : "user-applications-root"}
    Returns:
        The json data from the request. If more than 50 files were requested, the data is combined.
    """
    if client == "" or api == "" or feature == "" or segment == "" or template == "":
        raise Exception("A required parameter was blank.")

    my_content_type = "application/json"
    my_api_header = {"Content-Type": my_content_type, "Feature": feature}
    my_api_url = f"https://{client}.{api}/v2/users/export"

    if isinstance(segment, str):
        segment_type = "string"
        last_id_position = ""
    elif isinstance(segment, list):
        segment_type = "list"
        last_id_position = 0
    else:
        raise Exception("Not a valid segment or list")

    # Checking if the userlist is empty. If it is, return a fake result instead of an error from the server.
    if segment == [] or segment == "":
        return {"data": [], "meta": {"count": 0}}

    all_data_payload_results = ""
    results_data = ""
    run = True
    while run:
        if segment_type == "list":
            # This works since if we try and access a position in a list that doesn't exist, it returns nothing.
            my_payload = _create_api_data_payload(
                template,
                segment[last_id_position : last_id_position + 50],
                column_key,
                unwind,
            )
            last_id_position = last_id_position + 50
            if len(segment) < last_id_position:
                run = False
        else:
            my_payload = _create_api_data_payload(
                template, segment, last_id_position, column_key, unwind
            )

        result = requests.post(my_api_url, headers=my_api_header, data=my_payload)
        if result.status_code != 200:
            raise Exception(f"Export request failed for:{my_api_url} {result.content}")

        if "next_last_id" in result.json()["meta"] and segment_type == "string":
            last_id_position = result.json()["meta"]["next_last_id"]
        elif segment_type == "string":
            run = False

        if all_data_payload_results == "":
            all_data_payload_results = result.json()
        else:
            results_data = result.json()
            for x in results_data["data"]:
                all_data_payload_results["data"].append(x)
            all_data_payload_results["meta"]["count"] = (
                all_data_payload_results["meta"]["count"]
                + results_data["meta"]["count"]
            )
            del all_data_payload_results["meta"]["next_last_id"]

    return all_data_payload_results


def _create_api_data_payload(
    template_ID: str | dict,
    segment: str | list,
    last_id: str = "",
    column_key: str = "",
    unwind: dict = {},
) -> dict:
    """Used to create the data json request"""
    # Need to know if they passed a segment or a list.
    if isinstance(segment, list):
        user_id_list = segment
        segment = ""
    elif isinstance(segment, str):
        user_id_list = []

    my_payload = json.dumps(
        {
            "item": {
                "options": {"column_key": column_key, "unwind": unwind},
                "template": template_ID,
                "users": user_id_list,
                "segment": segment,
                "last_id": last_id,
            }
        }
    )
    return my_payload


def api_data_import(
    client: str, api: str, feature: str, template: str | dict, items: dict
) -> dict:
    """
    Provides a function to easily import data to an Element451 instance.
    **Uses the json and requests library.**

    Parameters:
        client (str): The name of the client instance
        api (str): The element API. Should be 'api.451.io'
        feature (str): The feature ID to access data
        template (str or dict/list): Can be an existing template guid or a template can be passed inline. Example: 'client.template.40291' or
        Items: The data to be update.
            Example with passed template: '[{"0": "john.smith@example.com","1": "John","2": "Smith"},....]
            Example with template guid[ {"user-email-address": "john.smith@example.com","user-first-name": "John","user-last-name": "Smith"},...]
    Returns:
        The json data from the request. If more than 50 files were requested, the data is combined.
    """
    if client == "" or api == "" or feature == "" or template == "" or items == "":
        raise Exception("A required parameter was blank.")
    my_content_type = "application/json"
    my_api_header = {"Content-Type": my_content_type, "Feature": feature}
    my_api_url = f"https://{client}.{api}/v2/users/import"
    my_payload = json.dumps({"item": {"template": template, "items": items}})

    result = requests.post(my_api_url, headers=my_api_header, data=my_payload)

    # Check if successful.
    if result.status_code != 201:
        raise Exception(f"Error while importing:{result.content}")

    return result.content


if __name__ == "__main__":
    # Used for loading environmental variables.
    import os
    from dotenv import load_dotenv

    load_dotenv()
    # The variable below are used for all import/export functions.
    my_client = os.getenv("my_client")  # "client"
    my_api = os.getenv("my_api")  # "api.451.io"
    my_feature = os.getenv("my_feature")  # "1234abc456efg"

    """Export Example 1"""
    my_segment = os.getenv("my_segment")  # "client.segments.4291"
    my_template = os.getenv("my_template_guid")  # "client.template.8571"
    print(api_data_request(my_client, my_api, my_feature, my_segment, my_template))

    """Export Example 2"""
    my_segment = os.getenv("my_segment")  # "client.segments.4291"
    my_template = {
        "columns": [
            {"field": "Email", "mode": "slug", "slug": "user-email-address"},
            {"field": "First Name", "mode": "slug", "slug": "user-first-name"},
            {"field": "Last Name", "mode": "slug", "slug": "user-last-name"},
            # { "field": "Note Type", "mode": "slug", "slug": "user-notes-note-type",
        ]
    }
    column_key = "slug"
    # To use the unwind feature, uncomment the Note Type field in the template and the field below, and add unwind to the function call.
    # unwind = {"root" : "user-notes-root"}
    print(
        api_data_request(
            my_client,
            my_api,
            my_feature,
            my_segment,
            my_template,
        )
    )

    """Search Example 1"""
    my_segment = os.getenv("my_segment")  # "client.segments.4291"
    project = ["first_name"]
    print(api_user_search(my_client, my_api, my_feature, my_segment, project))

    """Search Example 2"""
    my_segment = {
        "segment": {
            "users": {
                "filters": {
                    "type": "filter",
                    "target": "<mapping:user-email-address>",
                    "operator": "$match",
                    "value": "/element451/i",
                }
            }
        }
    }
    project = ["_id"]
    print(api_user_search(my_client, my_api, my_feature, my_segment, project))

    """Import Example 1"""
    my_template = os.getenv("my_template_guid")  # "client.templates.451",
    my_items = [{"0": "johnsmith@email.com", "1": "John", "2": "Smith"}]
    print(api_data_import(my_client, my_api, my_feature, my_template, my_items))

    """Import Example 2"""
    my_template = {
        "columns": [
            {
                "field": "Email",
                "mode": "slug",
                "slug": "user-email-address",
                "formula": "",
                "transformations": [],
                "scope": [],
                "validations": [],
                "type": "string",
                "range": ["unique"],
                "empty": "remove",
            },
            {
                "field": "First Name",
                "mode": "slug",
                "slug": "user-first-name",
                "formula": "",
                "transformations": [],
                "scope": [],
                "validations": [],
                "type": "string",
                "range": [],
                "empty": "remove",
            },
            {
                "field": "Last Name",
                "mode": "slug",
                "slug": "user-last-name",
                "formula": "",
                "transformations": [],
                "scope": [],
                "validations": [],
                "type": "string",
                "range": [],
                "empty": "remove",
            },
        ]
    }
    my_items = [{"0": "johnsmith@email.com", "1": "John", "2": "Smith"}]
    print(api_data_import(my_client, my_api, my_feature, my_template, my_items))
