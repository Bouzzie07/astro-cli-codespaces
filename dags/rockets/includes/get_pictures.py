import json
import requests


#A Python function that will parse the response and download all rocket pictures
def _get_pictures():
    with open("/usr/local/airflow/dags/rockets/launches/launches.json") as f:
        launches = json.load(f)

        image_urls = []

        for launch in launches["results"]:
            image_urls.append(launch["image"])
        
        for image_url in image_urls:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"/usr/local/airflow/dags/rockets/images/{image_filename}"

            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Download {image_url} to {target_file}")