# toes
We believe in this fight down to our toes. Generic home for endpoints for organizing APIs, webhooks, website utils, etc.

## Setup
Install docker and pipenv, then:

```bash
pipenv install -d
```

Run Tests:
```bash
pipenv run test
```

Run server:
```bash
pipenv run server
```
3. Open http://127.0.0.1:5000/ locally, e.g. [this mdata/debt endpoint](http://0.0.0.0:5000/mdata/debt?args=120000&profile_techsandbox_income_last_year=120000&profile_techsandbox_outstanding_student_loan_debt=60000)

4. (Optional) use ngrok to accept webhooks from the cloud. Install ngrok with `brew install ngrok`, then run
`ngrok http 5000`. ngrok will print out an internet-accessible HTTP/S URL that you can point your webhooks to
and they'll be forwarded . ngrok will also print out a localhost URL where you can see its web interface where
you can see and replay all incoming requests.

## How to deploy an updated function in the cloud
Use the `zappa-deploy.sh` script.
From this directory:

`../zappa-deploy.sh -p toes -s dev`: updates http://toes.elizabethwarren.codes/dev/...

`../zappa-deploy.sh -p toes -s prod`: updates http://toes.elizabethwarren.codes/prod/...
This is where prod traffic points to.

## Why is this called toes?

![toes in tweet](https://i.ibb.co/8s8yTL4/Screenshot-2019-07-16-14-32-45.png)

----------------------------

![toes in Peanuts](https://i.ibb.co/hy6N8px/toes.jpg)
