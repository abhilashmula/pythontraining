def is_prime(num):
    """
    Input: A Number
    Output: An print statement whether the number is prime or not
    """
    for n in range(2,num):
        if (num % n) ==0:
            print("Not Prime")
            break
    else:
            print("The Number is prime")




is_prime(39)
