# -*- coding: utf-8 -*-
import csv
import time
from datetime import date
import datetime
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tkinter import *
from threading import Thread
from tkinter import filedialog
import tkinter.messagebox as tkMessageBox
import shutil
from PIL import Image, ImageTk
import tkinter.ttk as ttk

def switch12():

    mainframe1.pack_forget()

    mainframe2.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))


def switch21():

    mainframe2.pack_forget()

    mainframe1.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



def switch23():

    mainframe2.pack_forget()

    mainframe3.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



def switch32():

    mainframe3.pack_forget()

    mainframe2.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



def switch34():

    mainframe3.pack_forget()

    mainframe4.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(10,10), pady=(0,0))



def switch43():

    mainframe4.pack_forget()

    mainframe3.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))





if __name__ == '__main__':
    root = Tk()
    root.title("Fund Document Scraper \\ Menu")
    root.geometry("%dx%d+0+0" % (1400, 900))
    root.resizable(1, 1)
    root.config(bg="#3399ff")

    title = Frame(root, bd=1, relief=SOLID)
    title.pack(side=TOP, pady=10)
    lbl_display = Label(title, text="Fund Document Verification Tool", font=('arial', 20))
    lbl_display.pack(padx=240, pady=(5,35))
    mainframe1 = Frame(root, bg='#3399ff')
    mainframe1.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



    NAME = StringVar()
    FNAME = StringVar()
    CODE = StringVar()
    DATE = StringVar()
    TYPEC = StringVar()
    PDF = IntVar()
    filename = None

    def handle_focus_in(_):
        date.delete(0, END)
        date.config(fg='black')

    def handle_focus_out(_):
        date.delete(0, END)
        date.config(fg='grey')
        date.insert(0, "Example: 12/31/2021")

    lbl_csvf = Label(mainframe1, bg='#3399ff', text="Input file", font=('arial', 12), bd=10)
    lbl_csvf.grid(row=0, pady=(75,0), sticky=W)
    csvf = Button(mainframe1, text='Select file', width=38)
    csvf.grid(row=0, column=1, pady=(75,0), sticky=W)

    lbl_date = Label(mainframe1, bg='#3399ff', text="Effective Date:", font=('arial', 12), bd=10)
    lbl_date.grid(row=1, sticky=W)
    date = Entry(mainframe1, bg='white', textvariable=DATE, font=('arial', 12), width=30, fg='grey')
    date.grid(row=1, column=1, sticky=W)
    date.insert(0, "MM/DD/YYYY")

    date.bind("<FocusIn>", handle_focus_in)
    date.bind("<FocusOut>", handle_focus_out)

    lbl_pdf = Label(mainframe1, bg='#3399ff', text="Scrape PDF's", font=('arial', 12), bd=10)
    lbl_pdf.grid(row=2, sticky=W)
    pdf = Checkbutton(mainframe1, variable=PDF, onvalue=1, offvalue=0)
    pdf.grid(row=2, column=1, sticky=W)
    pdf.config(bg="#3399ff")

    lbl_code = Label(mainframe1, bg='#3399ff', text="___________________________________________________________________________________________________________________________________", font=('arial', 12), bd=10)
    lbl_code.grid(row=3,columnspan=50, pady=(60,0), padx=(0,0))

    lbl_code = Label(mainframe1, bg='#3399ff', text="Manually add funds to Scraper", font=('arial', 12), bd=10)
    lbl_code.grid(row=4,columnspan=50, sticky=W)

    lbl_name = Label(mainframe1, bg='#3399ff', text="Provider Name:", font=('arial', 12), bd=10)
    lbl_name.grid(row=5, sticky=W)
    name = Entry(mainframe1, bg='white', textvariable=NAME, font=('arial', 12), width=30)
    name.grid(row=5, column=1, sticky=W)

    lbl_fname = Label(mainframe1, bg='#3399ff', text="Fund Name:", font=('arial', 12), bd=10)
    lbl_fname.grid(row=6, sticky=W)
    fname = Entry(mainframe1, bg='white', textvariable=FNAME, font=('arial', 12), width=30)
    fname.grid(row=6, column=1, sticky=W)

    lbl_typec = Label(mainframe1, bg='#3399ff', text="Fund Code Type:", font=('arial', 12), bd=10)
    lbl_typec.grid(row=7, sticky=W)
    code_types = ['ISIN', 'Cusip']
    typec = OptionMenu(mainframe1, TYPEC, *code_types)
    typec.config(width=38)
    typec.grid(row=7, column=1, sticky=W)
    TYPEC.set(code_types[0])

    lbl_code = Label(mainframe1, bg='#3399ff', text="Cusip/ISIN:", font=('arial', 12), bd=10)
    lbl_code.grid(row=8, sticky=W)
    code = Entry(mainframe1, bg='white', textvariable=CODE, font=('arial', 12), width=30)
    code.grid(row=8, column=1, sticky=W)


    add_btn = Button(mainframe1, text='Add Fund', width=10)
    add_btn.grid(row=9, column=1, sticky=W)
    add_btn.config(bg="LightBlue1")

    justone_btn = Button(mainframe1, text='Just this', width=20)
    #justone_btn.grid(row=10, column=1, columnspan=2)
    justone_btn.config(bg="LightBlue1")

    start_btn = Button(mainframe1, text='Start', width=20)
    #start_btn.grid(row=9, column=1, columnspan=2, pady=10)
    start_btn.config(bg="LightBlue1")

    download = Button(mainframe1, text="Download", width=20, bg="#009ACD")
    #download.grid(row=8, column=0, pady=10)
    download.config(bg="LightBlue1")
    download.config(state='disable')



    image11 = Image.open("right.png")
    image11 = image11.resize((25, 25), Image.ANTIALIAS)
    photo11 = ImageTk.PhotoImage(image11)
    right_button = Button(mainframe1, image=photo11, border=0, command=switch12)
    right_button.config(bg="#3399ff")
    right_button.place(x=670, y=725)

    image21 = Image.open("left.png")
    image21 = image21.resize((25, 25), Image.ANTIALIAS)
    photo21 = ImageTk.PhotoImage(image21)
    left_button = Button(mainframe1, image=photo21, border=0)
    left_button.config(bg="#3399ff")
    left_button.place(x=630, y=725)



    ##################################################################################

    mainframe2 = Frame(root, bg='#3399ff')
    # mainframe2.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))

    lbl_code = Label(mainframe2, bg='#3399ff', text="Check Fund Provider Website Whitelist", font=('arial', 10), bd=11)
    lbl_code.place(x=205, y=0)


    def DisplayData():
        global color_number
        color_number = True
        fetch = [['0',"provider1", "website1", 1],['1',"provider2", "website2", 0],['2',"provider3", "website3", 0],]
        for data in fetch:
            if color_number == True:
                treepe.insert('', 'end', values=(data[:-1]), tags=('gr',))
                color_number = False
            else:
                treepe.insert('', 'end', values=(data[:-1]))
                color_number = True


    def live_edit(event):
        global entryedit
        try:
            entryedit.destroy()
        except:
            pass
        column = treepe.identify_column(event.x)
        if column == "#4":
            return
        row = treepe.identify_row(event.y)
        cols = ('#0', ) + treepe.cget('columns')
        n_width = treepe.column(cols[int(column.replace("#",""))], 'width')
        x_long = 0
        for i in range(0,int(column.replace("#",""))):
            x_long = x_long + treepe.column(cols[i], 'width')
        if(row == ""):
            return
        item = treepe.selection()[0]
        item_text = treepe.item(item, "values")[int(column.replace("#",""))-1]
        entryedit = Text(mainframe2)
        entryedit.insert("1.0", item_text)
        entryedit.place(x=x_long, y=25+treepe.index(item)*30, width=n_width, height=30)
        entryedit.focus()

        def saveedit(event):
            value = entryedit.get("1.0",END).replace("\n","")
            entryedit.destroy()
            list_check = []
            if (column == "#2"):
                for child in treepe.get_children():
                    list_check.append(treepe.item(child)["values"][1])
                if list_check.count(value) > 0:
                    tkMessageBox.showerror("Error", "Full name exists. Please maintain a unique record.")
                else:
                    treepe.set(item, column=column, value=value)
            else:
                treepe.set(item, column=column, value=value)


        entryedit.bind("<FocusOut>", saveedit)
        entryedit.bind("<Return>", saveedit)


    def new_row():
        global color_number
        do = 1
        for child in treepe.get_children():
            valuesPE = treepe.item(child)["values"]
            if(valuesPE[1]==""):
                do = 0
                tkMessageBox.showerror("Error", "Please enter Full Name!")
                break
        if(do == 1):
            if color_number == True:
                treepe.insert('', len(treepe.get_children()), values=("", "", "", ""), tags=('gr',))
                color_number = False
            else:
                treepe.insert('', len(treepe.get_children()), values=("", "", "", ""))
                color_number = True
            treepe.update()
            child_id = treepe.get_children()[-1]
            treepe.selection_set(child_id)


    def fixed_map(option):
        return [elm for elm in style.map("Treeview", query_opt=option)
                if elm[:2] != ("!disabled", "!selected")]


    class CbTreeview(ttk.Treeview):
        def __init__(self, master=None, **kw):
            kw.setdefault('style', 'cb.Treeview')
            kw.setdefault('show', 'headings')  # hide column #0
            ttk.Treeview.__init__(self, master, **kw)
            # create checheckbox images
            self._im_checked = PhotoImage('checked',
                                             data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02#\x04\x82\xa9v\xc8\xef\xdc\x83k\x9ap\xe5\xc4\x99S\x96l^\x83qZ\xd7\x8d$\xa8\xae\x99\x15Zl#\xd3\xa9"\x15\x00;',
                                             master=self)
            self._im_unchecked = PhotoImage('unchecked',
                                               data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02\x1e\x04\x82\xa9v\xc1\xdf"|i\xc2j\x19\xce\x06q\xed|\xd2\xe7\x89%yZ^J\x85\x8d\xb2\x00\x05\x00;',
                                               master=self)
            style = ttk.Style(self)
            style.configure("cb.Treeview.Heading", font=(None, 13))
            # put image on the right
            style.layout('cb.Treeview.Row',
                         [('Treeitem.row', {'sticky': 'nswe'}),
                          ('Treeitem.image', {'side': 'right', 'sticky': 'e'})])

            # use tags to set the checkbox state
            self.tag_configure('checked', image='checked')
            self.tag_configure('unchecked', image='unchecked')

        def tag_add(self, item, tags):
            new_tags = tuple(self.item(item, 'tags')) + tuple(tags)
            self.item(item, tags=new_tags)

        def tag_remove(self, item, tag):
            tags = list(self.item(item, 'tags'))
            tags.remove(tag)
            self.item(item, tags=tags)

        def insert(self, parent, index, iid=None, **kw):
            item = ttk.Treeview.insert(self, parent, index, iid, **kw)
            self.tag_add(item, (item, 'unchecked'))
            self.tag_bind(item, '<ButtonRelease-1>',
                          lambda event: self._on_click(event, item))

        def _on_click(self, event, item):
            """Handle click on items."""
            if self.identify_row(event.y) == item:
                if self.identify_column(event.x) == '#4':  # click in 'Served' column
                    # toggle checkbox image
                    if self.tag_has('checked', item):
                        self.tag_remove(item, 'checked')
                        self.tag_add(item, ('unchecked',))
                    else:
                        self.tag_remove(item, 'unchecked')
                        self.tag_add(item, ('checked',))


    style = ttk.Style()
    style.configure('Treeview', rowheight=30)
    style.map("Treeview",
              foreground=fixed_map("foreground"),
              background=fixed_map("background"))



    scrollbary = Scrollbar(mainframe2, orient=VERTICAL)
    treepe = CbTreeview(mainframe2, columns=(
    "ID","Fund Provider", "Website Link", "Checkmark"), selectmode="extended",
                        height=100, yscrollcommand=scrollbary.set)

    treepe.tag_configure('gr', background='#F9F9F9')
    scrollbary.config(command=treepe.yview)
    scrollbary.pack(side=RIGHT, pady=(50,130), fill=Y)

    treepe.heading('ID', text="ID", anchor=W)
    treepe.heading('Fund Provider', text="Fund Provider", anchor=W)
    treepe.heading('Website Link', text="Website Link", anchor=W)
    treepe.heading('Checkmark', text="Checkmark", anchor=W)
    treepe.column('#0', stretch=NO, minwidth=0, width=0)
    treepe.column('#1', stretch=NO, minwidth=0, width=0)
    treepe.column('#2', stretch=NO, minwidth=0, width=340)
    treepe.column('#3', stretch=NO, minwidth=0, width=845)
    treepe.column('#4', stretch=NO, minwidth=0, width=145)
    treepe.pack(side=BOTTOM, fill=X, pady=(50,130))
    treepe.bind('<Double-1>', live_edit)


    DisplayData()


    add_btn = Button(mainframe2, text='Add Website', width=20, command=new_row)
    add_btn.place(x=1180, y=685)
    add_btn.config(bg="LightBlue1")

    add_btn = Button(mainframe2, text='Save Change', width=20)
    add_btn.place(x=1180, y=725)
    add_btn.config(bg="LightBlue1")




    image12 = Image.open("right.png")
    image12 = image12.resize((25, 25), Image.ANTIALIAS)
    photo12 = ImageTk.PhotoImage(image12)
    right_button = Button(mainframe2, image=photo12, border=0, command=switch23)
    right_button.config(bg="#3399ff")
    right_button.place(x=670, y=725)

    image22 = Image.open("left.png")
    image22 = image22.resize((25, 25), Image.ANTIALIAS)
    photo22 = ImageTk.PhotoImage(image22)
    left_button = Button(mainframe2, image=photo22, border=0, command=switch21)
    left_button.config(bg="#3399ff")
    left_button.place(x=630, y=725)


    mainframe2.pack_forget()


    ############################################################################################



    mainframe3 = Frame(root, bg='#3399ff')
    # mainframe3.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))


    NAME = StringVar()
    FNAME = StringVar()
    CODE = StringVar()
    DATE = StringVar()
    TYPEC = StringVar()
    PDF = IntVar()
    filename = None


    def handle_focus_in(_):
        date.delete(0, END)
        date.config(fg='black')

    def handle_focus_out(_):
        date.delete(0, END)
        date.config(fg='grey')
        date.insert(0, "Example: 12/31/2021")



    lbl_code = Label(mainframe3, bg='#3399ff', text="Additional Functions", font=('arial', 11), bd=10)
    lbl_code.place(x=570, y=0)

    add_btn = Button(mainframe3, text='Upload Whitelist', width=20)
    add_btn.place(x=480, y=50)
    add_btn.config(bg="LightBlue1")

    add_btn = Button(mainframe3, text='Download Whitelist', width=20)
    add_btn.place(x=480, y=90)
    add_btn.config(bg="LightBlue1")

    add_btn = Button(mainframe3, text='Download Scraper File', width=20)
    add_btn.place(x=480, y=130)
    add_btn.config(bg="LightBlue1")

    csvf = Button(mainframe3, text='Select file', width=38)
    csvf.place(x=665, y=50)




    run = Button(mainframe3, text="Run Scraper", width=20, height=2, bg="#009ACD")
    run.config(bg="LightBlue1")
    run.place(x=665, y=220)


    lbl_code = Label(mainframe3, bg='#3399ff', text="Compare to Fund Connect Fund Document Export", font=('arial', 11), bd=10)
    lbl_code.place(x=550, y=275)



    lbl_csvf = Label(mainframe3, bg='#3399ff', text="Input file", font=('arial', 12), bd=10)
    lbl_csvf.place(x=500, y=320)
    csvf = Button(mainframe3, text='Select file', width=38)
    csvf.place(x=665, y=325)


    lbl_date = Label(mainframe3, bg='#3399ff', text="Effective Date:", font=('arial', 12), bd=10)
    lbl_date.place(x=500, y=360)
    date = Entry(mainframe3, bg='white', textvariable=DATE, font=('arial', 12), width=30, fg='grey')
    date.place(x=665, y=365)
    date.insert(0, "MM/DD/YYYY")

    date.bind("<FocusIn>", handle_focus_in)
    date.bind("<FocusOut>", handle_focus_out)


    run = Button(mainframe3, text="Compare", width=20, height=2, bg="#009ACD")
    run.config(bg="LightBlue1")
    run.place(x=665, y=430)



    lbl_code = Label(mainframe3, bg='#3399ff', text="Click to next view after compare ran", font=('arial', 10), bd=10)
    lbl_code.place(x=550, y=660)


    image13 = Image.open("right.png")
    image13 = image13.resize((25, 25), Image.ANTIALIAS)
    photo13 = ImageTk.PhotoImage(image13)
    right_button = Button(mainframe3, image=photo13, border=0, command=switch34)
    right_button.config(bg="#3399ff")
    right_button.place(x=670, y=725)

    image23 = Image.open("left.png")
    image23 = image23.resize((25, 25), Image.ANTIALIAS)
    photo23 = ImageTk.PhotoImage(image23)
    left_button = Button(mainframe3, image=photo23, border=0, command=switch32)
    left_button.config(bg="#3399ff")
    left_button.place(x=630, y=725)

    ################################################################################################




    mainframe4 = Frame(root, bg='#3399ff')
    # mainframe4.pack(anchor=N, fill=BOTH, expand=True, side=LEFT,  padx=(40,10), pady=(0,0))



    def DisplayData():
        global color_number
        color_number = True
        fetch = [['0',"provider1", "website1", 1],['1',"provider2", "website2", 0],['2',"provider3", "website3", 0],]
        for data in fetch:
            if color_number == True:
                treepe.insert('', 'end', values=(data), tags=('gr',))
                color_number = False
            else:
                treepe.insert('', 'end', values=(data))
                color_number = True


    def live_edit(event):
        global entryedit
        try:
            entryedit.destroy()
        except:
            pass
        column = treepe.identify_column(event.x)
        row = treepe.identify_row(event.y)
        cols = ('#0', ) + treepe.cget('columns')
        n_width = treepe.column(cols[int(column.replace("#",""))], 'width')
        x_long = 0
        for i in range(0,int(column.replace("#",""))):
            x_long = x_long + treepe.column(cols[i], 'width')
        if(row == ""):
            return
        item = treepe.selection()[0]
        item_text = treepe.item(item, "values")[int(column.replace("#",""))-1]
        entryedit = Text(mainframe4)
        entryedit.insert("1.0", item_text)
        entryedit.place(x=x_long, y=25+treepe.index(item)*30, width=n_width, height=30)
        entryedit.focus()

        def saveedit(event):
            value = entryedit.get("1.0",END).replace("\n","")
            entryedit.destroy()
            list_check = []
            if (column == "#2"):
                for child in treepe.get_children():
                    list_check.append(treepe.item(child)["values"][1])
                if list_check.count(value) > 0:
                    tkMessageBox.showerror("Error", "Full name exists. Please maintain a unique record.")
                else:
                    treepe.set(item, column=column, value=value)
            else:
                treepe.set(item, column=column, value=value)


        entryedit.bind("<FocusOut>", saveedit)
        entryedit.bind("<Return>", saveedit)


    def new_row():
        global color_number
        do = 1
        for child in treepe.get_children():
            valuesPE = treepe.item(child)["values"]
            if(valuesPE[1]==""):
                do = 0
                tkMessageBox.showerror("Error", "Please enter Full Name!")
                break
        if(do == 1):
            if color_number == True:
                treepe.insert('', len(treepe.get_children()), values=("", "", "", ""), tags=('gr',))
                color_number = False
            else:
                treepe.insert('', len(treepe.get_children()), values=("", "", "", ""))
                color_number = True
            treepe.update()
            child_id = treepe.get_children()[-1]
            treepe.selection_set(child_id)


    def fixed_map(option):
        return [elm for elm in style.map("Treeview", query_opt=option)
                if elm[:2] != ("!disabled", "!selected")]


    class CbTreeview(ttk.Treeview):
        def __init__(self, master=None, **kw):
            kw.setdefault('style', 'cb.Treeview')
            kw.setdefault('show', 'headings')  # hide column #0
            ttk.Treeview.__init__(self, master, **kw)
            # create checheckbox images
            self._im_checked = PhotoImage('checked',
                                             data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02#\x04\x82\xa9v\xc8\xef\xdc\x83k\x9ap\xe5\xc4\x99S\x96l^\x83qZ\xd7\x8d$\xa8\xae\x99\x15Zl#\xd3\xa9"\x15\x00;',
                                             master=self)
            self._im_unchecked = PhotoImage('unchecked',
                                               data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02\x1e\x04\x82\xa9v\xc1\xdf"|i\xc2j\x19\xce\x06q\xed|\xd2\xe7\x89%yZ^J\x85\x8d\xb2\x00\x05\x00;',
                                               master=self)
            style = ttk.Style(self)
            style.configure("cb.Treeview.Heading", font=(None, 13))
            # put image on the right
            style.layout('cb.Treeview.Row',
                         [('Treeitem.row', {'sticky': 'nswe'}),
                          ('Treeitem.image', {'side': 'right', 'sticky': 'e'})])

            # use tags to set the checkbox state
            self.tag_configure('checked', image='checked')
            self.tag_configure('unchecked', image='unchecked')

        def tag_add(self, item, tags):
            new_tags = tuple(self.item(item, 'tags')) + tuple(tags)
            self.item(item, tags=new_tags)

        def tag_remove(self, item, tag):
            tags = list(self.item(item, 'tags'))
            tags.remove(tag)
            self.item(item, tags=tags)

        def insert(self, parent, index, iid=None, **kw):
            item = ttk.Treeview.insert(self, parent, index, iid, **kw)
            self.tag_add(item, (item, 'unchecked'))
            self.tag_bind(item, '<ButtonRelease-1>',
                          lambda event: self._on_click(event, item))

        def _on_click(self, event, item):
            """Handle click on items."""
            if self.identify_row(event.y) == item:
                if self.identify_column(event.x) == '#11':  # click in 'Served' column
                    # toggle checkbox image
                    if self.tag_has('checked', item):
                        self.tag_remove(item, 'checked')
                        self.tag_add(item, ('unchecked',))
                    else:
                        self.tag_remove(item, 'unchecked')
                        self.tag_add(item, ('checked',))



    style = ttk.Style()
    style.configure('Treeview', rowheight=30)
    style.map("Treeview",
              foreground=fixed_map("foreground"),
              background=fixed_map("background"))



    scrollbary = Scrollbar(mainframe4, orient=VERTICAL)
    treepe = CbTreeview(mainframe4, columns=(
    "ID","Fund Provider", "Fund Name", "ISIN", "Cusip", "Document", "Website Link", "Working", "Document Kind", "Link Currently in Fund", "Overwrite"), selectmode="extended",
                        height=100, yscrollcommand=scrollbary.set)

    treepe.tag_configure('gr', background='#F9F9F9')
    scrollbary.config(command=treepe.yview)
    scrollbary.pack(side=RIGHT, pady=(50,130), fill=Y)

    treepe.heading('ID', text="ID", anchor=W)
    treepe.heading('Fund Provider', text="Fund Provider", anchor=W)
    treepe.heading('Fund Name', text="Fund Name", anchor=W)
    treepe.heading('ISIN', text="ISIN", anchor=W)
    treepe.heading('Cusip', text="Cusip", anchor=W)
    treepe.heading('Document', text="Document", anchor=W)
    treepe.heading('Website Link', text="Website Link", anchor=W)
    treepe.heading('Working', text="Working", anchor=W)
    treepe.heading('Document Kind', text="Document Kind", anchor=W)
    treepe.heading('Link Currently in Fund', text="Link Currently in Fund", anchor=W)
    treepe.heading('Overwrite', text="Overwrite", anchor=W)
    treepe.column('#0', stretch=NO, minwidth=0, width=0)
    treepe.column('#1', stretch=NO, minwidth=0, width=0)
    treepe.column('#2', stretch=NO, minwidth=0, width=180)
    treepe.column('#3', stretch=NO, minwidth=0, width=215)
    treepe.column('#4', stretch=NO, minwidth=0, width=90)
    treepe.column('#5', stretch=NO, minwidth=0, width=90)
    treepe.column('#6', stretch=NO, minwidth=0, width=85)
    treepe.column('#7', stretch=NO, minwidth=0, width=250)
    treepe.column('#8', stretch=NO, minwidth=0, width=70)
    treepe.column('#9', stretch=NO, minwidth=0, width=120)
    treepe.column('#10', stretch=NO, minwidth=0, width=180)
    treepe.column('#11', stretch=NO, minwidth=0, width=80)
    treepe.pack(side=BOTTOM, fill=X, pady=(50,130))
    treepe.bind('<Double-1>', live_edit)


    DisplayData()


    add_btn = Button(mainframe4, text='Save', width=20, command=new_row)
    add_btn.place(x=1210, y=685)
    add_btn.config(bg="LightBlue1")

    add_btn = Button(mainframe4, text='Export', width=20)
    add_btn.place(x=1210, y=725)
    add_btn.config(bg="LightBlue1")




    image14 = Image.open("right.png")
    image14 = image14.resize((25, 25), Image.ANTIALIAS)
    photo14 = ImageTk.PhotoImage(image14)
    right_button = Button(mainframe4, image=photo14, border=0)
    right_button.config(bg="#3399ff")
    right_button.place(x=700, y=725)

    image24 = Image.open("left.png")
    image24 = image24.resize((25, 25), Image.ANTIALIAS)
    photo24 = ImageTk.PhotoImage(image24)
    left_button = Button(mainframe4, image=photo24, border=0, command=switch43)
    left_button.config(bg="#3399ff")
    left_button.place(x=660, y=725)









    root.mainloop()
