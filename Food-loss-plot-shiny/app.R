library(shiny)
library(data.table)
library(ggplot2)
library(DT)
library(scales)


load("/sws-shared-drive/Bayesian_food_loss/Saved_models/full_prediction_quantiles_df_2026.RData")

if (!exists("full_prediction_quantiles_df")) {
  stop("Object 'full_prediction_quantiles_df' is not loaded.")
}

# -------------------------------------------------------------------
# Prepare app data
# -------------------------------------------------------------------
app_data = as.data.table(full_prediction_quantiles_df)

app_data = app_data[
  ,
  .(
    year = as.integer(year),
    measureditemcpc = as.character(measureditemcpc),
    method = as.character(method),
    iso3 = as.character(iso3),
    CI_lower = as.numeric(CI_lower),
    median = as.numeric(median),
    CI_upper = as.numeric(CI_upper),
    country = as.character(country),
    sdg_region_name = as.character(sdg_region_name),
    region_l2 = as.character(region_l2),
    gfli_basket = as.character(gfli_basket)
  )
]

# Remove rows with missing predictions
app_data = app_data[!is.na(median) & !is.na(CI_lower) & !is.na(CI_upper)]

# Optional: use a nicer product label for now
app_data[, product_label := measureditemcpc]

setkey(app_data, method, country, measureditemcpc, year)

all_methods  = sort(unique(app_data$method))
all_countries = sort(unique(app_data$country))
all_products = sort(unique(app_data$product_label))
all_regions  = sort(unique(app_data$region_l2))
all_baskets  = sort(unique(app_data$gfli_basket))
year_min = min(app_data$year, na.rm = TRUE)
year_max = max(app_data$year, na.rm = TRUE)

default_country = if ("Afghanistan" %in% all_countries) "Afghanistan" else all_countries[1]
default_products = head(sort(unique(app_data[country == default_country, product_label])), 3)

# -------------------------------------------------------------------
# UI
# -------------------------------------------------------------------
ui = fluidPage(
  titlePanel("Bayesian food loss explorer"),
  
  sidebarLayout(
    sidebarPanel(
      selectInput(
        inputId = "method",
        label = "Dataset / method",
        choices = all_methods,
        selected = all_methods[1]
      ),
      
      selectizeInput(
        inputId = "countries",
        label = "Country / countries",
        choices = all_countries,
        selected = default_country,
        multiple = TRUE,
        options = list(placeholder = "Select one or more countries")
      ),
      
      selectizeInput(
        inputId = "products",
        label = "Product code(s)",
        choices = all_products,
        selected = default_products,
        multiple = TRUE,
        options = list(placeholder = "Select one or more product codes")
      ),
      
      sliderInput(
        inputId = "year_range",
        label = "Year range",
        min = year_min,
        max = year_max,
        value = c(year_min, year_max),
        step = 1,
        sep = ""
      ),
      
      selectizeInput(
        inputId = "regions",
        label = "Filter by region_l2 (optional)",
        choices = all_regions,
        selected = NULL,
        multiple = TRUE
      ),
      
      selectizeInput(
        inputId = "baskets",
        label = "Filter by basket (optional)",
        choices = all_baskets,
        selected = NULL,
        multiple = TRUE
      ),
      
      selectInput(
        inputId = "facet_by",
        label = "Facet plot by",
        choices = c("None", "Country", "Product", "Country + Product"),
        selected = "Country"
      ),
      
      checkboxInput(
        inputId = "show_ribbon",
        label = "Show confidence interval ribbon",
        value = TRUE
      ),
      
      numericInput(
        inputId = "max_series",
        label = "Max series before warning",
        value = 12,
        min = 1,
        step = 1
      ),
      
      downloadButton("download_data", "Download filtered data")
    ),
    
    mainPanel(
      tabsetPanel(
        tabPanel(
          "Plot",
          br(),
          plotOutput("loss_plot", height = "700px"),
          br(),
          verbatimTextOutput("selection_summary")
        ),
        tabPanel(
          "Table",
          br(),
          DTOutput("loss_table")
        )
      )
    )
  )
)

# -------------------------------------------------------------------
# Server
# -------------------------------------------------------------------
server = function(input, output, session) { #session only in the case I will need it later
  
  filtered_data = reactive({
    d = copy(app_data)
    
    d = d[
      method == input$method &
        year >= input$year_range[1] &
        year <= input$year_range[2]
    ]
    
    if (!is.null(input$countries) && length(input$countries) > 0) {
      d = d[country %in% input$countries]
    }
    
    if (!is.null(input$products) && length(input$products) > 0) {
      d = d[product_label %in% input$products]
    }
    
    if (!is.null(input$regions) && length(input$regions) > 0) {
      d = d[region_l2 %in% input$regions]
    }
    
    if (!is.null(input$baskets) && length(input$baskets) > 0) {
      d = d[gfli_basket %in% input$baskets]
    }
    
    d[order(country, product_label, year)]
  })
  
  output$selection_summary = renderText({
    d = filtered_data()
    
    if (nrow(d) == 0) {
      return("No data for the current selection.")
    }
    
    n_countries = uniqueN(d$country)
    n_products  = uniqueN(d$product_label)
    n_years     = uniqueN(d$year)
    n_rows      = nrow(d)
    
    paste0(
      "Rows: ", format(n_rows, big.mark = ","),
      " | Countries: ", n_countries,
      " | Products: ", n_products,
      " | Years: ", n_years
    )
  })
  
  output$loss_plot = renderPlot({
    d = filtered_data()
    
    validate(
      need(nrow(d) > 0, "No data available for the selected filters.")
    )
    
    n_countries = uniqueN(d$country)
    n_products  = uniqueN(d$product_label)
    
    # Choose legend label automatically
    if (n_countries == 1 && n_products >= 1) {
      d[, legend_label := product_label]
    } else if (n_products == 1 && n_countries >= 1) {
      d[, legend_label := country]
    } else {
      d[, legend_label := paste(country, product_label, sep = " | ")]
    }
    
    n_series = uniqueN(d$legend_label)
    
    validate(
      need(
        n_series <= input$max_series,
        paste0(
          "You selected ", n_series, " series. ",
          "Reduce the number of countries/products or facet the plot."
        )
      )
    )
    
    p = ggplot(
      d,
      aes(
        x = year,
        y = median,
        group = legend_label
      )
    )
    
    if (isTRUE(input$show_ribbon)) {
      p <- p +
        geom_ribbon(
          aes(
            ymin = CI_lower,
            ymax = CI_upper,
            fill = legend_label
          ),
          alpha = 0.18,
          colour = NA
        )
    }
    
    p = p +
      geom_line(aes(color = legend_label), linewidth = 0.9) +
      geom_point(aes(color = legend_label), size = 1.8) +
      scale_y_continuous(labels = percent_format(accuracy = 1)) +
      labs(
        x = "Year",
        y = "Food loss ratio",
        color = "Series",
        fill = "Series",
        title = "Median food loss with confidence intervals"
      ) +
      theme_minimal(base_size = 13) +
      theme(
        legend.position = "bottom",
        panel.grid.minor = element_blank()
      )
    
    if (input$facet_by == "Country") {
      p <- p + facet_wrap(~ country, scales = "free_y")
    } else if (input$facet_by == "Product") {
      p <- p + facet_wrap(~ product_label, scales = "free_y")
    } else if (input$facet_by == "Country + Product") {
      p <- p + facet_grid(country ~ product_label, scales = "free_y")
    }
    
    p
  })
  
  output$loss_table = renderDT({
    d = filtered_data()
    
    datatable(
      d[
        ,
        .(
          year,
          country,
          iso3,
          product_code = product_label,
          method,
          CI_lower,
          median,
          CI_upper,
          sdg_region_name,
          region_l2,
          gfli_basket
        )
      ],
      rownames = FALSE,
      filter = "top",
      extensions = "Buttons",
      options = list(
        pageLength = 25,
        scrollX = TRUE,
        dom = "Bfrtip",
        buttons = c("copy", "csv", "excel")
      )
    )
  })
  
  output$download_data = downloadHandler(
    filename = function() {
      paste0("food_loss_filtered_", Sys.Date(), ".csv")
    },
    content = function(file) {
      fwrite(filtered_data(), file)
    }
  )
}

shinyApp(ui = ui, server = server)
