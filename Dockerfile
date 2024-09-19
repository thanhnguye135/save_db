FROM node:22

WORKDIR /usr/src/app

COPY package*.json ./ yarn*.lock ./

RUN yarn install 

RUN yarn global add pm2

COPY . .

EXPOSE 3000

CMD ["pm2-runtime", "start", "ecosystem.config.js"]