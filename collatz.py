def recursion(number, counter = 1):
        if number == 1:
                return counter
        if number%2 == 0:
                number = number/2
        else:
                number = (3*number) + 1
        counter += 1
        return recursion(number, counter)


def longest_chain(max=1000000):
        longest_number=1
        longest_chain=1
        number_chain_dict={longest_number:longest_chain}
        for x in range(2, max):
                if x in number_chain_dict:
                        y=number_chain_dict.get(longest_chain)
                        longest_chain += y
                else:
                        counter=recursion(x)
                        if counter > longest_chain:
                                longest_chain=counter
                                longest_number = x
                                number_chain_dict[longest_number]=longest_chain
        return(longest_number)
