import random
givers = [
'Nikhil',
'Vincent',
'Bryan',
'Sinead',
'Chris'
]

def genSecretSanta():
    result = []
    restart = True
    print("Start")
    while restart: 
        restart = False
        receivers = givers[:]
        print("Second")
        for i in range(len(givers)):
            giver = givers[i]
            print("Loop:"+ str(i))
            print(giver)
            # Pick a random reciever
            receiver = random.choice(receivers)
            print(receiver)
            # restart the generation when we've got to the last giver and its the same as the reciever, 
            if (giver == receiver and i == (len(givers) - 1)):
                restart = True
                print("Break")
                break
            else:
                while receiver == giver:
                	print("same")
                	receiver = random.choice(receivers)
                receivers.remove(receiver)
                print("Removed")
    for r in result:
    	print(r)


def main():
	genSecretSanta()

if __name__ == '__main__':
	main()
