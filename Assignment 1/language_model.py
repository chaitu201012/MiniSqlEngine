import re 
import time 
import os
from xml.etree.ElementTree import tostring 


class Tokenizer:

    def __init__(self,file):
        self.filename = open(file,encoding="utf8")
        self.text = self.filename.read()
        self.data = re.split('(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s',self.text)
        self.tokens = []
        self.tokenize_file = open('2020201012_tokenize.txt','w+')

    
    def closefiles(self):

        self.filename.close()
        self.tokenize_file.close()
        
    def tokenize(self,sentence):

        self.tokenize_file.write(sentence)
        self.tokenize_file.write('\n')

        

    def clean(self):
        #print(self.data[0:1])
        #print('\n')
        punc = ('(', ')', '?', ':', ';', ',', '.', '!', '/', '-','"', "'", '{', '}', '[', ']', '_', '+', '=')
        for i in range(len(self.data)):
            encoded_string = self.data[i].encode("ascii", "ignore")
            tweet = encoded_string.decode()
            tweet = re.sub(r'\n',' ',tweet)
            specialchars = ('{','}','!','#','$','%','^','&','*','(',')','<','>','+','-','/','~','`',':',';','_','=','|','?','@','[',']')
            
            tweet = re.sub(r"http:(\S+)", "<URL>", tweet)
            tweet = re.sub(r"www.(\S+).com", "<URL>", tweet)
            tweet = re.sub(r"WWW.(\S+).com", "<URL>", tweet)
            tweet = re.sub(r"@(\w+)", "<MENTION>", tweet)
            tweet =  re.sub(r'#(\w+)','<HASHTAG>',tweet)
            #tweet = re.sub("\S*\d\S*", "", tweet).strip()
            #tweet = re.sub('[^A-Za-z]+', ' ', tweet)
            #tweet = re.sub('HASHTAG','<HASHTAG>',tweet)
            #tweet = re.sub('MENTION','<MENTION>',tweet)
            #tweet = re.sub('URL','<URL>',tweet)
            for k in specialchars:  ## remove duplicate characters 
                tweet = re.sub(r'\{}+'.format(k),k,tweet)

            
            newtweet = ""

            for w in tweet.split(' '):
                if len(w)>=2 and w[-1] in punc and type(w)!=list:
                    w = w[:-1]+" "+w[-1]
                newtweet+=w+" "
            

            tweet = newtweet.strip()
                        
            
            self.data[i] = tweet
            self.tokenize(tweet)
            self.tokens.extend(self.data[i].split())

        #print(len(self.tokens))
        #print("*"*20)
        #print(self.data[0])
        #print('\n')

    def getLenOfTokens(self):

        return len(self.tokens)

    def getData(self):

        return self.data

    def getTokens(self):

        return self.tokens


class ngrams:

    def __init__(self,filename):
        # saving tuple as key is easy compared to string 
        self.file = filename
        self.tokenizer = Tokenizer(filename)
        self.tokenizer.clean()
        self.totalTokens =  self.tokenizer.getLenOfTokens()
        self.data = self.tokenizer.getData()
        self.unigrams = {}
        self.bigrams = {}
        self.trigrams = {}
        self.fourgrams = {}

        self.ngram = {}

        self.get_unigram()
        self.get_bigrams()
        self.get_trigrams()
        self.get_fourgrams()



    def get_unigram(self):

        for line in self.data:
            words = line.split()
            for w in words:
                self.unigrams[w] = 1 if w not in self.unigrams.keys() else self.unigrams[w]+1
        
        self.ngram[1] = self.unigrams
        return self.unigrams

    def get_bigrams(self):

        for line in self.data:
            words  = line.split()

            for i in range(len(words)-1):
                self.bigrams[(words[i],words[i+1])] =1 if (words[i],words[i+1]) not in self.bigrams.keys() else self.bigrams[(words[i],words[i+1])]+1

        self.ngram[2] = self.bigrams
        return self.bigrams

    def get_trigrams(self):

        for line in self.data:
            words  = line.split()

            for i in range(len(words)-2):
                self.trigrams[(words[i],words[i+1],words[i+2])] = 1 if (words[i],words[i+1],words[i+2]) not in self.trigrams.keys() else self.trigrams[(words[i],words[i+1],words[i+2])]+1
        
        self.ngram[3] = self.trigrams
        return self.trigrams

    def get_fourgrams(self):

        for line in self.data:
            words  = line.split()

            for i in range(len(words)-3):
                self.fourgrams[(words[i],words[i+1],words[i+2],words[i+3])] = 1 if (words[i],words[i+1],words[i+2],words[i+3]) not in self.fourgrams.keys() else self.fourgrams[(words[i],words[i+1],words[i+2],words[i+3])]+1
        
        self.ngram[4] =self.fourgrams
        return self.fourgrams

    def get_ngrams(self):

        return self.ngram

    def get_data(self):

        return self.data

    def get_vocabsize(self):

        return self.totalTokens



class kneyser:

    def __init__(self,filename):

        self.file = filename
        self.ngrams = ngrams(filename)
        self.V= self.ngrams.get_vocabsize()
        self.ngram = self.ngrams.get_ngrams()
        #self.get_unigrams = self.ngrams.get_unigrams()
        #self.get_bigrams = self.ngrams.get_bigrams()
        #self.get_trigrams = self.ngrams.get_trigrams()
        #self.fourgrams = self.ngrams.get_fourgrams()

        self.data = self.ngrams.get_data()

        self.train_data = self.data[:-1000]

        self.testdata = self.data[-1000:]

        self.trainfile = open('train.txt','w+')

        self.avg_perplexity = 0.0 
        count=0
        #print(len(self.train_data))
        for sent in self.train_data:
            count+=1
            p = self.algorithm(sent)
            self.avg_perplexity+=p
            self.trainfile.write(sent+"\t"+str(p)+"\n")
            #break
        
        print("number of sentences in europarl corpus are := ", count )
        print("Average perplexity score of the corpus is = ",self.avg_perplexity/count)


    def countcontext(self,context,n):
        count =0

        for gram in self.ngram[n].keys():
            if context == gram[:-1]:
                count+=1
        return count


    def calculatefirsterm(self,i,n,d,sentence):
        if n==1 :

            if sentence[i] in self.ngram[1].keys():
                return max(self.ngram[n][sentence[i]]-d,0)/(len(self.ngram[n]))
            
            return 0

        else:

            context = tuple(sentence[i-n+1:i])
            curgram = tuple(sentence[i-n+1:i+1])
            #print(self.ngram.keys())
            if curgram in self.ngram[n].keys():

                if context in self.ngram[n-1].keys():
            
                    return max(self.ngram[n][curgram]-d,0)/(self.ngram[n-1][context])
                
            return 0


    def calculatesecondterm(self,i,n,d,sentence):

        
        if n==1:
            return d/len(self.ngram[n])

        else:

            context = tuple(sentence[i-n+1:i])
            #denom = self.ngram[n-1][context] if 
            curgram = tuple(sentence[i-n+1:i+1])

            ## for lambda term if context is not present i.e count(wi-1)=0 i.e denom is zero then we need to 
            ## apply lamda(unknown)/total types in the  ngram 

            if context not in self.ngram[n-1].keys():
                #instead of self.V we can use unique_value counts of lower order grams 
                return (d*(len(self.ngram[n])))*self.knesersmoothing(i,n-1,sentence)

            else: 

                return (d*(self.countcontext(context,n))/len(self.ngram[n]))*self.knesersmoothing(i,n-1,sentence)


               

    def knesersmoothing(self,i,n,sentence)->float:
        ## Reference: https://medium.com/@dennyc/a-simple-numerical-example-for-kneser-ney-smoothing-nlp-4600addf38b8

        d = 0.50
        firstterm = 0.0
        secondterm = 0.0

        firstterm = self.calculatefirsterm(i,n,d,sentence)
        secondterm = self.calculatesecondterm(i,n,d,sentence)

        return firstterm+secondterm
        
    def algorithm(self,sentence):

        perplex=1;
        sentence = re.split('[;,. :]',sentence)
        #print(sentence)

        for i in range(3,len(sentence)-1):
            perplex*= self.knesersmoothing(i,4,sentence)
        #print(perplex,end='\n')
        return pow(perplex,-1/len(sentence)) if perplex>0.0 and len(sentence)>0 else 0.0


        

        ## let us try to decompose the gigantic equation into the three terms 



class witten:

    def __init__(self,filename):

        self.file = filename
        self.ngrams = ngrams(filename)
        self.V= self.ngrams.get_vocabsize()
        self.ngram = self.ngrams.get_ngrams()
        #self.get_unigrams = self.ngrams.get_unigrams()
        #self.get_bigrams = self.ngrams.get_bigrams()
        #self.get_trigrams = self.ngrams.get_trigrams()
        #self.fourgrams = self.ngrams.get_fourgrams()

        self.data = self.ngrams.get_data()

        self.train_data = self.data[:-1000]

        self.testdata = self.data[-1000:]

        self.trainfile = open('wittenbelltrain.txt','w+')

        self.avg_perplexity = 0.0 
        count=0
        print(len(self.train_data))
        for sent in self.train_data:
            count+=1
            p = self.algorithm(sent)
            self.avg_perplexity+=p
            self.trainfile.write(sent+"\t"+str(p)+"\n")
            if count%10==0:
                print(self.avg_perplexity/count)
                break
            #break
        
        print("number of sentences in europarl corpus are := ", count )
        print("Average perplexity score of the corpus is = ",self.avg_perplexity/count)


    
    def count_unique(self,context,n):

        count =0
        seen=set()

        for gram in self.ngram[n].keys():
            if context == gram[:-1]:
                count+=1
                #seen.add(gram[-1])
        return count

    def count_values(self,n):
        count=0
        for key,values in self.ngram[n].items():
            count+=values
        
        return count


    def calculatefirstterm(self,i,n,d,sentence):

        if n==1 :

            if sentence[i] in self.ngram[1].keys():
                return self.ngram[1][sentence[i]]/self.V
            
            return 0

        else:
           
            curgram = tuple(sentence[i-n+1:i+1])
            #print(self.ngram.keys())
            try:
                if curgram in self.ngram[n].keys():

                    return self.ngram[n][curgram]/len(self.ngram[n])

                return 0
            
            except KeyError:
                print(n)
                print(self.ngram[n].keys())

        
    def calculatesecondterm(self,i,n,d,sentence):

        if n==1:
            if sentence[i] in self.ngram[1].keys():
                return self.ngram[1][sentence[i]]/(len(self.ngram[1]))
            
            return d/(len(self.ngram[1]))

        else:

            context = tuple(sentence[i-n+1:i])
            #if context in self.ngram[n-1].keys():
            if context in self.ngram[n-1].keys():
                uniquewords = self.count_unique(context,n)
                oneminuslambdaterm = uniquewords/(uniquewords+self.count_values(n))

                return oneminuslambdaterm*self.wittenbell(i,n-1,sentence)
            
            else :

                return (d*(len(self.ngram[n-1]))/self.count_values(n))*self.wittenbell(i,n-1,sentence)
 


    def wittenbell(self,i,n,sentence):

        d=0.75

        firstterm = self.calculatefirstterm(i,n,d,sentence)
        secondterm = self.calculatesecondterm(i,n,d,sentence)

        return firstterm+secondterm


    def algorithm(self,sentence):

        perplex=1;
        sentence = re.split('[;,. :]',sentence)
        #print(sentence)

        for i in range(3,len(sentence)-1):
            perplex*= self.wittenbell(i,4,sentence)
        #print(perplex,end='\n')
        return perplex**(-1/len(sentence))





 




        



        
tokenizer = Tokenizer('general-tweets.txt')
tokenizer.clean()

start = time.time()
kn = kneyser('europarl-corpus.txt')
print("time taken for witten bell on europarl is =", time.time()-start)

print(kn.avg_perplexity)






