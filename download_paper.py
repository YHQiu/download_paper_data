import os
import requests
import xmltodict
import json
from tqdm import tqdm

def download_data_and_save_as_json():
    url = "https://storage.googleapis.com/arxiv-dataset?delimiter=/&maxResults=10000"

    # Send an HTTP GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the XML response to a dictionary
        xml_data = xmltodict.parse(response.content)

        # Extract the 'Contents' part and convert it to a list of dictionaries
        contents = xml_data['ListBucketResult']['Contents']
        if not isinstance(contents, list):
            contents = [contents] if contents else []

        # Write the extracted data to a JSON file
        with open('./files.json', 'w') as json_file:
            json.dump(contents, json_file, indent=4)

        print("Data saved to ./files.json.")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)

def clean_and_save_to_jsonl(contents, output_filename):
    pdf_contents = []

    for content in contents:
        key = content['Key']
        if key.endswith('.pdf'):
            pdf_contents.append({"key": key})

    # Create the 'downloaded_files' directory if it doesn't exist
    if not os.path.exists('./downloaded_files'):
        os.makedirs('./downloaded_files')

    # Write the cleaned data to a JSONL file
    with open(output_filename, 'w') as jsonl_file:
        for pdf_content in pdf_contents:
            jsonl_file.write(json.dumps(pdf_content) + '\n')

    print("Data cleaned and saved to", output_filename)

def download_files(contents, output_filename):
    total_count = len(contents)
    downloaded_data = {"total_count": total_count, "download_index": 0}

    for i, content in enumerate(tqdm(contents, desc="Downloading")):
        key = content['key']
        download_url = f"https://storage.googleapis.com/arxiv-dataset/{key}"
        download_path = os.path.join(output_filename, os.path.basename(key))

        # Download the file
        response = requests.get(download_url, stream=True)
        with open(download_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        # Update the download index
        downloaded_data["download_index"] = i + 1

        # Write the download progress to a JSON file
        with open('downloaded.json', 'w') as json_file:
            json.dump(downloaded_data, json_file, indent=4)
        
        print("Downloaded file saved at:", download_path)

if __name__ == "__main__":
    # Check if the JSON file exists
    if not os.path.exists('./files.json'):
        # If it doesn't exist, download the data and save as JSON
        download_data_and_save_as_json()
    else:
        print("JSON file 'files.json' already exists. Skipping download.")

    # Load the contents from the JSON file
    with open('./files.json', 'r') as json_file:
        data = json.load(json_file)
        contents = data

    # Clean the data and save to JSONL
    clean_and_save_to_jsonl(contents, './file_name.jsonl')

    # Download the PDF files and track progress
    download_files(contents, './downloaded_files')
