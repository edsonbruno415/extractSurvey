import pandas as pd
import dask.dataframe as dd
import unicodedata
import json
import os

FILE_PATH = "FILE_PATH"
BUCKET_URL = "BUCKET_URL"

if not os.path.isfile(FILE_PATH):
    print('Downloading data...')
    ddf = dd.read_parquet(BUCKET_URL, columns=["date", "body", "custom"])
    ddf.to_csv(FILE_PATH, single_file=True, index=False)

print('Reading file...')
df = dd.read_csv(FILE_PATH).sort_values('date')

def normalizeText(text):
    return unicodedata.normalize("NFKD", text).encode("ASCII", "ignore")

def getDate(dict):
    try:
        return dict["surveyAnswersSaved"]["start"]
    except:
        return None
    
def getCorrelationId(dict):
    try:
        return dict["correlationId"]
    except:
        return None

def getSlug(dict):
    try:
        return dict["surveyAnswersSaved"]["slug"]
    except:
        raise Exception("Dont have Slug skipped this data!!!")

def getProposal(dict):
    try:
        return dict["additionalData"]["setupSurvey"]["user"]["proposal"]
    except:
        return None

def getSignatureStatus(dict):
    try:
        signatures = dict["additionalData"]["setupSurvey"]["user"]["signatures"]
        template = ""
        for signature in signatures:
            template += "{}\n".format(signature["status"])
        return template
    except:
        return None

def getProductName(dict):
    try:
        signatures = dict["additionalData"]["setupSurvey"]["user"]["signatures"]
        template = ""
        for signature in signatures:
            template += "{}\n".format(signature["productName"])
        return template
    except:
        return None

def getSignatureType(dict):
    try:
        signatures = dict["additionalData"]["setupSurvey"]["user"]["signatures"]
        template = ""
        for signature in signatures:
            template += "{}\n".format(signature["typeCD"])
        return template
    except:
        return None

def getSignatures(dict):
    try:
        signatures = dict["additionalData"]["setupSurvey"]["user"]["signatures"]
        template = ""
        for signature in signatures:
            template += "{}\n".format(signature["signatureId"])
        return template
    except:
        return None

def getFullName(dict):
    try:
        return dict["additionalData"]["setupSurvey"]["user"]["name"]
    except:
        return None

def getLastSpecialist(dict):
    try:
        return dict["surveyAnswersSaved"]["lastSpecialist"]
    except:
        return None

def getQuestion(dict, numberQuestion):
    try:
        return dict["surveyAnswersSaved"]["answers"][numberQuestion - 1]["question"]
    except:
        return None

def getAnswer(dict, numberAnswer):
    try:
        return dict["surveyAnswersSaved"]["answers"][numberAnswer - 1]["answer"]["text"]
    except:
        return None

def create_survey():
    return dict(
        {
            "correlationId": [],
            "InicioPesquisa": [],
            "Slug": [],
            "Proposta": [],
            "AssinaturaStatus": [],
            "AssinaturaNome": [],
            "AssinaturaTipo": [],
            "Assinatura": [],
            "Nome": [],
            "UltimoBot": [],
            "Pergunta1": [],
            "Resposta1": [],
            "Pergunta2": [],
            "Resposta2": [],
            "Pergunta3": [],
            "Resposta3": [],
        }
    )

listSurveys = {
    "pesquisa-scob": create_survey(),
    "pesquisa-selfcare": create_survey(),
    "pesquisa-pre": create_survey(),
    "pesquisa-onb": create_survey(),
    "pesquisa-cpf-engajamento": create_survey(),
    "pesquisa-post-paid": create_survey(),
}

for row in df.iterrows():
    try:
        body = json.loads(row[-1]["body"])
        custom = json.loads(row[-1]["custom"])
        currentData = {
            "correlationId": getCorrelationId(custom),
            "InicioPesquisa": getDate(custom),
            "Slug": getSlug(custom),
            "Proposta": getProposal(body),
            "AssinaturaStatus": getSignatureStatus(body),
            "AssinaturaNome": getProductName(body),
            "AssinaturaTipo": getSignatureType(body),
            "Assinatura": getSignatures(body),
            "Nome": getFullName(body),
            "UltimoBot": getLastSpecialist(custom),
            "Pergunta1": getQuestion(custom, 1),
            "Resposta1": getAnswer(custom, 1),
            "Pergunta2": getQuestion(custom, 2),
            "Resposta2": getAnswer(custom, 2),
            "Pergunta3": getQuestion(custom, 3),
            "Resposta3": getAnswer(custom, 3),
        }

        surveyPivot = listSurveys[currentData["Slug"]]
        for key in surveyPivot.keys():
            surveyPivot[key].append(currentData[key])
    except:
        print("Skipping add data:", row[0])

scob = dd.DataFrame.from_dict(listSurveys["pesquisa-scob"], npartitions=1)
selfcare = dd.DataFrame.from_dict(listSurveys["pesquisa-selfcare"], npartitions=1)
pre = dd.DataFrame.from_dict(listSurveys["pesquisa-pre"], npartitions=1)
onb = dd.DataFrame.from_dict(listSurveys["pesquisa-onb"], npartitions=1)
cpf = dd.DataFrame.from_dict(listSurveys["pesquisa-cpf-engajamento"], npartitions=1)
postpaid = dd.DataFrame.from_dict(listSurveys["pesquisa-post-paid"], npartitions=1)

scobWithoutDuplicates = scob.drop_duplicates(subset=["correlationId", "InicioPesquisa"],keep='last').drop(columns=["correlationId"])
selfcareWithoutDuplicates = selfcare.drop_duplicates(subset=["correlationId", "InicioPesquisa"],keep='last').drop(columns=["correlationId"])
preWithoutDuplicates = pre.drop_duplicates(subset=["correlationId", "InicioPesquisa"],keep='last').drop(columns=["correlationId"])
onbWithoutDuplicates = onb.drop_duplicates(subset=["correlationId", "InicioPesquisa"],keep='last').drop(columns=["correlationId"])
cpfWithoutDuplicates = cpf.drop_duplicates(subset=["correlationId", "InicioPesquisa"],keep='last').drop(columns=["correlationId"])
postpaidWithoutDuplicates = postpaid.drop_duplicates(subset=["correlationId", "InicioPesquisa"],keep='last').drop(columns=["correlationId"])

print("Writing files...")
dd.to_csv(
    scobWithoutDuplicates, "./extract/pesquisa-scob.csv", single_file=True, index=False, encoding="utf-8"
)
dd.to_csv(
    selfcareWithoutDuplicates,"./extract/pesquisa-selfcare.csv", single_file=True, index=False, encoding="utf-8",
)
dd.to_csv(
    preWithoutDuplicates, "./extract/pesquisa-pre.csv", single_file=True, index=False, encoding="utf-8"
)
dd.to_csv(
    onbWithoutDuplicates, "./extract/pesquisa-onb.csv", single_file=True, index=False, encoding="utf-8"
)
dd.to_csv(
    cpfWithoutDuplicates,"./extract/pesquisa-cpf-engajamento.csv", single_file=True, index=False, encoding="utf-8",
)
dd.to_csv(
    postpaidWithoutDuplicates,"./extract/pesquisa-post-paid.csv", single_file=True, index=False, encoding="utf-8",
)