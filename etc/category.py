categories = [
    {
        "category": "Mahlad ja joogid",
        "sub-categories": [
            "Mahlad ja -kontsentraadid, siirupid",
            "Muud joogid",
            "Alkoholivabad joogid",
            "Energiajoogid",
            "Kakaod, kakaojoogid",
            "Karastusjoogid, toonikud",
            "Kohvid",
            "Smuutid, värsked mahlad",
            "Spordijoogid",
        ],
    },
    {
        "category": "Alkohoolsed joogid",
        "sub-categories": [
            "Long Drink",
            "Siider",
            "Ölled",
            "Kange Alkohol",
            "Džinnid",
            "Konjakid, brändid",
            "Liköörid",
            "Liköörveinid",
            "Muud kanged alkohoolsed joogid",
            "Punased veinid",
            "Roosad veinid",
            "Rummid",
            "Õlled, siidrid, segud, kokteilid",
            "Šampanjad, vahuveinid",
        ],
    },
    {
        "category": "Juust",
        "sub-categories": ["Juust", "Delikatessjuustud", "Juustud", "Määrdejuustud"],
    },
    {
        "category": "Külmutatud ja jahedad tooted",
        "sub-categories": [
            "KÜlmutatud Tooted",
            "Jahutatud valmistoidud",
            "Jogurtid, jogurtijoogid",
            "Jäätised",
            "Kohukesed",
            "Kohupiimad, kodujuustud",
            "Külmutatud liha- ja kalatooted",
            "Külmutatud köögiviljad, marjad, puuviljad",
            "Külmutatud tainad ja kondiitritooted",
            "Külmutatud valmistooted",
        ],
    },
    {
        "category": "Kuivained",
        "sub-categories": [
            "Kuivained",
            "Paja- ja nuudliroad",
            "Hommikuhelbed, müslid, kiirpudrud",
            "Jahud",
            "Kuivsupid ja -kastmed",
            "Leivad",
            "Maitseained",
            "Makaronid",
            "Näkileivad",
            "Pähklid ja kuivatatud puuviljad",
            "Riisid",
            "Saiad",
            "Saiakesed, stritslid, kringlid",
            "Sepikud, kuklid, lavašid",
            "Sipsid",
            "Puljongid",
        ],
    },
    {
        "category": "Margariinid Ja õlid",
        "sub-categories": ["Margariinid Ja õlid", "Võid, margariinid", "Õlid, äädikad"],
    },
    {
        "category": "Viljad ja muud värsked tooted",
        "sub-categories": [
            "Puu  Ja Juurviljad",
            "Köögiviljad, juurviljad",
            "Maitsetaimed, värsked salatid, piprad",
            "Salatid",
            "Seened",
            "Õunad, pirnid",
        ],
    },
    {"category": "Munad", "sub-categories": ["Munad"]},
    {
        "category": "Piimatooted",
        "sub-categories": ["Piimatooted", "Suupisted (Piim)", "Piimad, koored"],
    },
    {
        "category": "Lihad ja kalatooted",
        "sub-categories": [
            "Grillvorstid, verivorstid",
            "Hakkliha",
            "Keedu- ja suitsuvorstid, viinerid",
            "Linnuliha",
            "Muud kalatooted",
            "Muud lihatooted",
            "Sealiha",
            "Singid, rulaadid",
            "Soolatud ja suitsutatud kalatooted",
            "Sushi",
        ],
    },
    {
        "category": "Hoidised",
        "sub-categories": [
            "Hoidised",
            "Ketšupid, tomatipastad, kastmed",
            "Majoneesid, sinepid",
        ],
    },
    {
        "category": "Kommid ja muud magusad",
        "sub-categories": [
            "Kommikarbid",
            "Kommipakid",
            "Koogid, rullbiskviidid, tainad",
            "Küpsised",
            "Magusad hoidised",
            "Magustoidud",
            "Maiustused, küpsised, näksid",
            "Muud magustoidud",
            "Muud maiustused",
            "Šokolaadid",
        ],
    },
    {"category": "Lastetoidud", "sub-categories": ["Lastetoidud"]},
    {"category": "Maailma köök", "sub-categories": ["Maailma köök"]},
]


def category_parser(prod_category):
    for item in categories:
        for category in item["sub-categories"]:
            if prod_category == category:
                return item["category"]
    return "N/A"
