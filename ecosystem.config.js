module.exports = {
  apps: [
    {
      name: "app",
      exec_mode: "cluster",
      script: "./app.js",
      instance: 1,
      watch: true,
    },
    {
      name: "worker",
      exec_mode: "cluster",
      script: "./worker.js",
      instance: 5,
      watch: true,
    },
  ],
};
