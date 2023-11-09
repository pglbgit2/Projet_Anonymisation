import json

def generate_json():
    data = {}
    for i in range(1, 111):
        inner_data = {}
        for j in range(10, 21):
            key = f"2015-{j:02d}"
            inner_data[key] = [str(i)]
        data[str(i)] = inner_data

    return json.dumps(data, indent=4)

# Utilisation de la fonction pour générer le JSON
result = generate_json()

with open("tresanonym.json", "w") as outfile:
    outfile.write(result)
