package controllers

import play.api._
import play.api.mvc._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

import utils.SparkMLLibUtility

object Application extends Controller {

  def index = Action {
    Future{SparkSQL.runQuery("select count(*) from %TABLE% where datatime='2014121801'", "20141218","2014121801")}
    Ok(views.html.index(""))
  }

}
