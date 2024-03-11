input_string = "AAAAAAAAAAAAAAAAAAAAAAAAABBBACCI"
count = 1
encoded_string = ""
for item in range(1, len(input_string)):
    if input_string[item] == input_string[item -1]:
        count += 1
    else:
        encoded_string += str(count) + input_string[item -1]
        count = 1
encoded_string += str(count) + input_string[-1]
print(encoded_string)