# Simple MapReduce
## Word Length Count
### command format
hadoop jar [.jar file] [class name] [input dir] [output dir]
### sample input
The quick brown fox jumps   
over the lazy dogs.
### sample output
3 3  
5 3  
4 3

## N-consecutive initial count
### command format
hadoop jar [.jar file] [class name] [input dir] [output dir] [N]
### sample input
‘The quick brown fox jumps    
over the lazy dogs.

N = 2
### sample output
T q 1  
q b 1  
b f 1  
f j 1  
j o 1  
o t 1  
t l 1  
l d 1
## N-consecutive initial relative frequencies
### command format
hadoop jar [.jar file] [class name] [input dir] [output dir] [N] [θ]
### sample input
the quick brown fox jumps over the lazy dogs.  
the Quick Brown fox jumps Over the Lazy dogs.  
the Quick Brown fox Jumps Over the Lazy ‘‘dogs’’

N = 2, θ = 0.6
### sample output
f j 0.66666667  
q b 1  
b f 1  
o t 1  
l d 1  
d t 1  
Q B 1  
B f 1  
O t 1  
L d 1  
J O 1
