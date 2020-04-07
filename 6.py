# Resilient Distributed Datasets = immutable
# pip3 install pyspark
# pip install pyspark

# immutable = final = setelah di inisialisasi maka dia gak bisa diganti
# ringan
# bisa di track
# kerjanya itu pararel

from pyspark import SparkContext as sc

my_accumulator = 0

def check_blank(line):
    global my_accumulator
    if line == "":
        my_accumulator += 1


def main():
    global my_accumulator
    context = sc.getOrCreate()
    # transformation        -> kita dari rdd sebelumnya kita membuat rdd baru
    # action                -> kita mengambil hasil dari rdd yang kita punya
    # cara untuk membuat RDD
    # pararellize
    # textFile
    # 1. Read a file named “The Frog Prince.txt” and count how many lines the file has
    text = context.textFile("./The Frog Prince.txt")
    print("Line count :",text.count())
    # 2. Count how many times the “Prince” word occurs in the file
    # transformation map
    # action countByValue
    prince_count = text.map(lambda x: "Prince" in x).countByValue() # x.contains("prince") / strstr("prince")
    print("Prince count :",prince_count)
    # 3. Filter any lines contains “master” or “love” in the file and print each of them
    # transformation filter
    master_love_count = text.filter(lambda x: "master" in x or "love" in x)
    # collect() -> untuk mengambil isinya
    print(master_love_count.collect())
    # 4. Extract all unique words in the file and display it in single array
    # transformation flatMap
    # ["hello world", "how are you"]
    # map -> [["hello","world"],["how","are","you"]]
    # flatMap -> ["hello","world","how","are","you"]
    all_data = text.flatMap(lambda x: x.split(" ")).distinct()
    print(all_data.collect())
    # 5. Using accumulators, count how many blank lines available in the file
    # accumulator -> untuk melakukan aggregate dengan kondisi tertentu
    my_accumulator = context.accumulator(0)
    text.foreach(check_blank)
    print("Blank line count :",my_accumulator)

main()