import requests

def extract(world: str = 'Louisoix', *, timeout_s: int = 10) -> list[dict[str, int | str]]:
    response = requests.get(
        'https://universalis.app/api/v2/extra/stats/most-recently-updated',
        params = {"world" : world}, timeout = timeout_s
    )
    response.raise_for_status()
    
    data = response.json()

    if 'items' not in data:
        raise KeyError ("Universalis response missing 'items' key")
    else:
        return data['items']

if __name__ == "__main__":
    print(extract())


# gets recently updated infromation from the api and returns data['items'] on function call.

# use type hint for editorial and readibilty purposes. 
# assume each step fails, handle that via error handling
# use name guards