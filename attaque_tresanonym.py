import json

def tres_anonym():
    data = {}
    for i in range(1, 111):
        inner_data = {}
        for j in range(10, 21):
            key = f"2015-{j:02d}"
            inner_data[key] = [str(i)]
        data[str(i)] = inner_data

    result = json.dumps(data, indent=4)
    
    with open("tresanonym.json", "w") as outfile:
        outfile.write(result)

def autofill():
    data = {}
    for i in range(1, 111):
        inner_data = {}
        for j in range(10, 21):
            key = f"2015-{j:02d}"
            inner_data[key] = [str(565+(i+j-11)%110)]
        data[str(i)] = inner_data

    result = json.dumps(data, indent=4)
    
    with open("autofill.json", "w") as outfile:
        outfile.write(result)

if __name__== "__main__":
    autofill()