import os
import ijson.backends.yajl2_cffi as ijson
import json

def membersByActivity(conversation):
    messageCounts = {m["name"]: len(list(filter(lambda e: e["author"] == m["id"], conversation["messages"]))) for m in conversation["members"]}
    activitySorted = [" ".join([s[0], str(s[1])]) for s in reversed(sorted(messageCounts.items(), key=lambda k:k[1]))]
    return activitySorted

def wordsByFrequency(conversation):
    import collections, itertools
    words = list(itertools.chain.from_iterable([w for w in (m["content"].lower().split(" ") for m in conversation["messages"] if m["content"])]))
    frequencies = collections.defaultdict(int)
    for w in words: frequencies[w] += 1
    return frequencies
    
def convertToText(conversation):
    from datetime import datetime
    return "\n".join(datetime.fromtimestamp(int(m["timestamp"])/1000000).strftime("%D %H:%M ") + (lambda a: a[0] if len(a) > 0 else "Unkown")([mem["name"] for mem in conversation["members"] if mem["id"] == m["author"]]) + ": "+m["content"] for m in sorted(conversation["messages"], key=lambda m:m["timestamp"]) if m["content"])

def markovSentence(conversation):
    import markovify
    return markovify.NewlineText("\n".join(map(lambda m: m["content"], conversation["messages"]))).make_sentence()

files = []

print("Actions:\n1. Process Logs\n2. Analyze Proccesed Logs")
action = input("Enter number of action to perform: ")
if action == "1":
    print("="*50+"\nLog Files: ")
    i = 1
    for fn in os.listdir("."):
        if fn[-5:] == ".json" and fn[-len("_conversations.json"):] != "_conversations.json":
            files.append(fn)
            print(str(i)+". "+fn)
            i += 1
    fn = files[int(input("Enter number of file to read logs from: "))-1]
    data = ijson.parse(open(fn, "rb"))
    conversations = []
    conversation = False
    message = False
    member = False
    for prefix, event, value in data:
        if prefix == "conversations.item.conversation.conversation_id.id":
            print("Processing chat...")
            if conversation:
                if len(conversation["members"]) == 2 and conversation["name"] == None: conversation["name"] = (lambda a,b: a[0]["name"] if len(a) > 0 else b)(list(filter(lambda m: m["id"] != conversation["self"], conversation["members"])), "")
                conversations.append(conversation)
            conversation = {"id": value, "name": None, "members": [], "messages": [], "self": None}
        if prefix == "conversations.item.conversation.conversation.name": conversation["name"] = value
        if prefix == "conversations.item.conversation.conversation.participant_data.item.id.gaia_id":
            if member: conversation["members"].append(member)
            member = {"id": value, "name": ""}
        if prefix == "conversations.item.conversation.conversation.self_conversation_state.self_read_state.participant_id.gaia_id" and conversation["self"] == None : conversation["self"] = value
        if prefix == "conversations.item.conversation.conversation.participant_data.item.fallback_name": member["name"] = value
        if prefix == "conversations.item.events.item.sender_id.gaia_id":
            if message: conversation["messages"].append(message)
            message = {"author": value, "content": "" , "timestamp": ""}
        if prefix == "conversations.item.events.item.timestamp": message["timestamp"] = value
        if prefix == "conversations.item.events.item.chat_message.message_content.segment.item.text" and event == "string":
            message["content"] += value
    
    print("Writing to file...")
    json.dump(conversations, open(".".join(fn.split(".")[:-1]) + "_conversations"+".json", "w"))
    print("Done.")
elif action == "2":
    print("="*50+"\nProcessed Files: ")
    i = 1
    for fn in os.listdir("."):
        if fn[-len("_conversations.json"):] == "_conversations.json":
            files.append(fn)
            print(str(i)+". "+fn)
            i += 1
    fn = files[int(input("Enter number of file to read logs from: "))-1]
    print("Loading conversations...", end=" ")
    conversations = json.load(open(fn))
    print("done.")
    
    print("Conversations:")
    i = 1
    conversations = list(sorted(conversations, key=lambda c:len(c["messages"])))
    for conversation in sorted(conversations, key=lambda c:len(c["messages"])):
        print(str(i)+". "+conversation["name"] + " (" + str(len(conversation["messages"])) + " messages)")
        i += 1
        
    conversation = conversations[int(input("Enter number of conversation to analyze: "))-1]

    
    print("Most common words:")
    for i, (x, _) in enumerate(zip(reversed(sorted(wordsByFrequency(conversation).items(), key=lambda k:k[1])), range(10))):
        print(str(i+1) + ". " + x[0])
        
    print("Members by activity:")
    for i, m in enumerate(membersByActivity(conversation)):
        print(str(i+1) + ". " + m)
        
    print("Markov chain sentence:")
    print(markovSentence(conversation))
