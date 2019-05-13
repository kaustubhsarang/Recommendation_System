import csv
user_id = [];
business_id=[];
star=[];
names=[];
business_id2=[];
with open('G:\My Drive\UIC SEM 2\Big data\Assignment\project\\review_new.csv', 'rb') as f:
    reader = csv.reader(f,quotechar='"',delimiter=',');
    your_list = list(reader);
    for x in your_list:
        user_id.append(x[0]);
        business_id.append(x[1]);
        star.append(x[3]);
with open('G:\My Drive\UIC SEM 2\Big data\Assignment\project\\business_clean.csv', 'rb') as f:
    reader = csv.reader(f, quotechar='"', delimiter=',');
    your_list2 = list(reader);
    for x in your_list2:
        business_id2.append(x[0]);
        names.append(x[1]);

dict_user=dict();
for i in range(0, len(user_id)):
    dict_user[user_id[i]]=i;

dict_business = dict();
for i in range(0, len(business_id2)):
    dict_business[business_id2[i]] = i;

with open('G:\My Drive\UIC SEM 2\Big data\Assignment\project\\ratings_int.csv', mode='wb') as employee_file:
    employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for i,val in enumerate(your_list):
        employee_writer.writerow([dict_user.get(user_id[i]), dict_business.get(val[1]), star[i]]);
3
    # for i in range(0,len(your_list)):
    #     employee_writer.writerow([dict_user.get(user_id[i]),dict_business.get(business_id2[i]),star[i]]);


with open('G:\My Drive\UIC SEM 2\Big data\Assignment\project\\business_int.csv', mode='wb') as bus_file:
    bus_writer = csv.writer(bus_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for i in range(0,len(your_list2)):
        bus_writer.writerow([dict_business.get(business_id2[i]),names[i]]);