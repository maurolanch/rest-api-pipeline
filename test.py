import requests

response = requests.get(

"https://iansaura.com/api/datasets.php?email=maurolanchm@gmail.com&key=ds_bad7c752c680880ff1a504337d97a54b&type=ecommerce&rows=1000"


)

data = response.json()

print(data)



