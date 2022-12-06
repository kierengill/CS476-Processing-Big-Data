with open('input.txt') as f:
    lines = f.readlines()

params = ['Hackathon', 'Dec', 'Chicago' , 'Java', 'Engineers']
for param in params:
    count = 0
    param_ = param.lower()
    for line in lines:
        line = line.lower()
        if line.find(param_) != -1 :
            count += 1
    print(param, count)


